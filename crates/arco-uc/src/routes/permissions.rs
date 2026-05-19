//! Permission endpoints for the UC facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, OriginalUri, Path, Query, State};
use axum::http::Method;
use axum::http::StatusCode;
use axum::routing::get;

use arco_catalog::authz::compiler::{CompiledPermissionRow, CompiledPermissionSet};
use serde::Deserialize;
use serde_json::json;

use crate::context::UnityCatalogRequestContext;
use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
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
    )
)]
pub async fn get_permissions(
    State(state): State<UnityCatalogState>,
    Extension(_ctx): Extension<UnityCatalogRequestContext>,
    Path((securable_type, full_name)): Path<(String, String)>,
    Query(query): Query<GetPermissionsQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let Some(compiled_permissions) = state.compiled_permissions.as_ref() else {
        let payload = json!({
            "privilege_assignments": []
        });
        return (StatusCode::OK, Json(payload));
    };
    let Ok(compiled_permissions) = compiled_permissions.read() else {
        let payload = json!({
            "privilege_assignments": []
        });
        return (StatusCode::OK, Json(payload));
    };
    let assignments = permission_assignments(
        &compiled_permissions,
        &securable_type,
        &full_name,
        query.principal.as_deref(),
    );
    let payload = json!({
        "privilege_assignments": assignments,
        "ledger_watermark": compiled_permissions.ledger_watermark,
        "group_snapshot_version": compiled_permissions.group_snapshot_version,
    });
    (StatusCode::OK, Json(payload))
}

fn permission_assignments(
    compiled_permissions: &CompiledPermissionSet,
    securable_type: &str,
    full_name: &str,
    principal_filter: Option<&str>,
) -> Vec<serde_json::Value> {
    let object_type = securable_type.to_ascii_uppercase();
    compiled_permissions
        .rows
        .iter()
        .filter(|row| row.object_type.eq_ignore_ascii_case(&object_type))
        .filter(|row| row.object_id == full_name)
        .filter(|row| {
            principal_filter
                .map(|principal| row.principal_id == principal)
                .unwrap_or(true)
        })
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
    super::common::known_but_unsupported(&method, &uri)
}
