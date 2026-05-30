//! Permission endpoints for the UC facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, OriginalUri, Path, Query, State};
use axum::http::Method;
use axum::http::StatusCode;
use axum::routing::get;

use arco_catalog::authz::privileges::Privilege;
use arco_catalog::{
    CatalogError,
    authz::compiler::{CompiledPermissionRow, CompiledPermissionSet},
};
use serde::Deserialize;
use serde_json::json;

use crate::context::UnityCatalogRequestContext;
use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::error::UnityCatalogResult;
use crate::routes::common::{self, require_authz};
use crate::state::UnityCatalogState;

#[derive(Debug, Deserialize)]
/// Query parameters for `GET /permissions/{securable_type}/{full_name}`.
pub struct GetPermissionsQuery {
    /// Optional principal filter.
    pub principal: Option<String>,
}

/// Permission route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new().route(
        "/permissions/:securable_type/:full_name",
        get(get_permissions).patch(update_permissions),
    )
}

/// `GET /permissions/{securable_type}/{full_name}` (Scope A).
///
/// # Errors
///
/// Returns an error when the securable cannot be resolved or permission reads are denied.
#[utoipa::path(
    get,
    path = "/permissions/{securable_type}/{full_name}",
    tag = "Permissions",
    params(
        ("securable_type" = String, Path, description = "Securable type"),
        ("full_name" = String, Path, description = "Securable full name"),
    ),
    responses(
        (status = 200, description = "Get permissions"),
        (status = 403, description = "Permission read denied", body = UnityCatalogErrorResponse),
    )
)]
pub async fn get_permissions(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path((securable_type, full_name)): Path<(String, String)>,
    Query(query): Query<GetPermissionsQuery>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let object_type = securable_type.to_ascii_uppercase();
    let object_id = resolve_permission_object_id(&state, &ctx, &object_type, &full_name).await?;
    require_authz(
        &state,
        &ctx,
        &object_id,
        &object_type,
        Privilege::Manage,
        "get_permissions",
    )?;

    let Some(compiled_permissions) = state.compiled_permissions.as_ref() else {
        let payload = json!({
            "privilege_assignments": []
        });
        return Ok((StatusCode::OK, Json(payload)));
    };
    let Ok(compiled_permissions) = compiled_permissions.read() else {
        let payload = json!({
            "privilege_assignments": []
        });
        return Ok((StatusCode::OK, Json(payload)));
    };
    let assignments = permission_assignments(
        &compiled_permissions,
        &object_type,
        &object_id,
        query.principal.as_deref(),
    );
    let payload = json!({
        "privilege_assignments": assignments,
        "ledger_watermark": compiled_permissions.ledger_watermark,
        "group_snapshot_version": compiled_permissions.group_snapshot_version,
    });
    Ok((StatusCode::OK, Json(payload)))
}

async fn resolve_permission_object_id(
    state: &UnityCatalogState,
    ctx: &UnityCatalogRequestContext,
    object_type: &str,
    full_name: &str,
) -> UnityCatalogResult<String> {
    let Some(reader) = common::authoritative_catalog_reader(state, ctx).await? else {
        return Ok(full_name.to_string());
    };

    let resolved = match object_type {
        "CATALOG" => {
            map_optional_object(reader.get_catalog(full_name).await)?.map(|catalog| catalog.id)
        }
        "SCHEMA" => {
            let Some((catalog_name, schema_name)) = parse_schema_full_name(full_name) else {
                return Ok(full_name.to_string());
            };
            map_optional_object(reader.list_schemas(catalog_name).await.map(|schemas| {
                schemas
                    .into_iter()
                    .find(|schema| schema.name == schema_name)
            }))?
            .map(|schema| schema.id)
        }
        "TABLE" => {
            let Some((catalog_name, schema_name, table_name)) = parse_table_full_name(full_name)
            else {
                return Ok(full_name.to_string());
            };
            map_optional_object(
                reader
                    .get_table_in_schema(catalog_name, schema_name, table_name)
                    .await,
            )?
            .map(|table| table.id)
        }
        _ => None,
    };

    Ok(resolved.unwrap_or_else(|| full_name.to_string()))
}

fn map_optional_object<T>(
    result: Result<Option<T>, CatalogError>,
) -> UnityCatalogResult<Option<T>> {
    match result {
        Ok(value) => Ok(value),
        Err(CatalogError::NotFound { .. }) => Ok(None),
        Err(err) => Err(common::map_catalog_error(err)),
    }
}

fn parse_schema_full_name(full_name: &str) -> Option<(&str, &str)> {
    let mut parts = full_name.split('.');
    let catalog_name = parts.next()?;
    let schema_name = parts.next()?;
    parts
        .next()
        .is_none()
        .then_some((catalog_name, schema_name))
}

fn parse_table_full_name(full_name: &str) -> Option<(&str, &str, &str)> {
    let mut parts = full_name.split('.');
    let catalog_name = parts.next()?;
    let schema_name = parts.next()?;
    let table_name = parts.next()?;
    parts
        .next()
        .is_none()
        .then_some((catalog_name, schema_name, table_name))
}

fn permission_assignments(
    compiled_permissions: &CompiledPermissionSet,
    securable_type: &str,
    full_name: &str,
    principal_filter: Option<&str>,
) -> Vec<serde_json::Value> {
    let object_type = securable_type.to_ascii_uppercase();
    compiled_permissions
        .rows()
        .iter()
        .filter(|row| row.object_type.eq_ignore_ascii_case(&object_type))
        .filter(|row| row.object_id == full_name)
        .filter(|row| principal_filter.is_none_or(|principal| row.principal_id == principal))
        .map(permission_assignment)
        .collect()
}

fn permission_assignment(row: &CompiledPermissionRow) -> serde_json::Value {
    json!({
        "principal": row.principal_id,
        "privileges": [row.privilege.as_str()],
        "inherited_from": row.source_object_id,
        "source_principal": row.source_principal_id,
        "grant_option": row.grant_option,
    })
}

/// `PATCH /permissions/{securable_type}/{full_name}` (known UC operation; currently unsupported).
#[utoipa::path(
    patch,
    path = "/permissions/{securable_type}/{full_name}",
    tag = "Permissions",
    params(
        ("securable_type" = String, Path, description = "Securable type"),
        ("full_name" = String, Path, description = "Securable full name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn update_permissions(method: Method, uri: OriginalUri) -> UnityCatalogError {
    common::known_but_unsupported(&method, &uri)
}
