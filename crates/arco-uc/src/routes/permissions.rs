//! Permission endpoints for the UC facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, OriginalUri, Path, Query, State};
use axum::http::Method;
use axum::http::StatusCode;
use axum::routing::get;

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
    State(_state): State<UnityCatalogState>,
    Extension(_ctx): Extension<UnityCatalogRequestContext>,
    Path((_securable_type, _full_name)): Path<(String, String)>,
    Query(_query): Query<GetPermissionsQuery>,
) -> (StatusCode, Json<serde_json::Value>) {
    let payload = json!({
        "privilege_assignments": []
    });
    (StatusCode::OK, Json(payload))
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
