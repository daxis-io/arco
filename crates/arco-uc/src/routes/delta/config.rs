//! Delta catalog protocol configuration route for the Unity Catalog facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, Query, State};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};
use crate::routes::{common, preview};
use crate::state::UnityCatalogState;

/// Delta catalog protocol config route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new().route("/delta/v1/config", get(get_config))
}

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[serde(default)]
pub(crate) struct DeltaCatalogConfigQuery {
    catalog: Option<String>,
    #[serde(rename = "protocol-versions")]
    protocol_versions: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub(crate) struct DeltaCatalogConfigResponse {
    endpoints: Vec<String>,
    #[serde(rename = "protocol-version")]
    protocol_version: String,
}

/// Delta endpoint signatures served for the negotiated protocol version.
///
/// List of supported endpoint signatures are in the form "METHOD /v1/path".
/// Paths are relative to the catalog API root (/api/2.1/unity-catalog/delta).
fn delta_endpoints_for_protocol_version(version: &str) -> Vec<String> {
    match version {
        "1.0" => vec![],
        _ => Vec::new(),
    }
}

/// Get delta catalog configuration.
///
/// # Errors
///
/// Returns [`UnityCatalogError`] when request validation fails, or catalog is missing.
#[utoipa::path(
    get,
    path = "/delta/v1/config",
    tag = "DeltaConfiguration",
    params(
        ("catalog" = String, Query, description = "Catalog name"),
        ("protocol-versions" = String, Query, description = "Comma-separated list of highest protocol versions the client supports per major version
            (e.g., 1.1,2.3 means the client supports 1.0-1.1 and 2.0-2.3).
            The server selects the highest mutually supported protocol version and returns endpoints for that version.")
    ),
    responses(
        (status = 200, description = "Configuration retrieved successfully", body = DeltaCatalogConfigResponse),
        (status = 400, description = "Bad request.", body = UnityCatalogErrorResponse),
        (status = 401, description = "Unauthorized.", body = UnityCatalogErrorResponse),
        (status = 403, description = "Forbidden.", body = UnityCatalogErrorResponse),
        (status = 404, description = "Not found.", body = UnityCatalogErrorResponse),
        (status = 500, description = "Internal server error.", body = UnityCatalogErrorResponse)
    )
)]
pub(crate) async fn get_config(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    query: Query<DeltaCatalogConfigQuery>,
) -> UnityCatalogResult<Json<DeltaCatalogConfigResponse>> {
    let Query(query) = query;
    let _ = &query.extra;
    let _ = &query.protocol_versions;

    let catalog = preview::require_identifier(query.catalog, "catalog")?;

    // Verify catalog exists
    let reader = common::authoritative_catalog_reader(&state, &ctx)
        .await?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("catalog not found: {catalog}"),
        })?;
    reader
        .get_catalog(&catalog)
        .await
        .map_err(common::map_catalog_error)?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("catalog not found: {catalog}"),
        })?;

    tracing::debug!(
        tenant = %ctx.tenant,
        workspace = %ctx.workspace,
        request_id = %ctx.request_id,
        catalog = %catalog,
        "unity catalog get delta catalog config from authoritative catalog state"
    );

    // Input protocol-versions is ignored and the negotiated version is
    // hardcoded to 1.0, as it is the only supported protocol version.
    let protocol_version = "1.0".to_string();

    Ok(Json(DeltaCatalogConfigResponse {
        endpoints: delta_endpoints_for_protocol_version(&protocol_version),
        protocol_version,
    }))
}
