//! Temporary credential routes for the Unity Catalog facade.

use axum::Json;
use axum::extract::Extension;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::context::UnityCatalogRequestContext;
use crate::error::{UnityCatalogError, UnityCatalogErrorResponse, UnityCatalogResult};

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[schema(title = "GenerateTemporaryTableCredentialRequestBody")]
#[serde(default)]
pub(crate) struct GenerateTemporaryTableCredentialRequestBody {
    table_id: Option<String>,
    operation: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Deserialize, utoipa::ToSchema)]
#[schema(title = "GenerateTemporaryPathCredentialRequestBody")]
#[serde(default)]
pub(crate) struct GenerateTemporaryPathCredentialRequestBody {
    url: Option<String>,
    operation: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

/// Generates temporary table credentials (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    post,
    path = "/temporary-table-credentials",
    tag = "TemporaryCredentials",
    request_body = GenerateTemporaryTableCredentialRequestBody,
    responses(
        (status = 200, description = "Successful response.", body = Value),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_temporary_table_credentials(
    _ctx: Extension<UnityCatalogRequestContext>,
    payload: Json<GenerateTemporaryTableCredentialRequestBody>,
) -> UnityCatalogResult<Json<Value>> {
    let Json(payload) = payload;
    let _ = (payload.table_id, payload.operation, payload.extra);
    Err(UnityCatalogError::NotImplemented {
        message:
            "POST /temporary-table-credentials is scaffolded in preview; implementation is pending."
                .to_string(),
    })
}

/// Generates temporary path credentials (preview scaffolding).
///
/// # Errors
///
/// Returns [`UnityCatalogError::NotImplemented`] while preview scaffolding is active.
#[utoipa::path(
    post,
    path = "/temporary-path-credentials",
    tag = "TemporaryCredentials",
    request_body = GenerateTemporaryPathCredentialRequestBody,
    responses(
        (status = 200, description = "Successful response.", body = Value),
        (status = 501, description = "Endpoint is scaffolded but not yet implemented.", body = UnityCatalogErrorResponse),
    )
)]
pub(crate) async fn post_temporary_path_credentials(
    _ctx: Extension<UnityCatalogRequestContext>,
    payload: Json<GenerateTemporaryPathCredentialRequestBody>,
) -> UnityCatalogResult<Json<Value>> {
    let Json(payload) = payload;
    let _ = (payload.url, payload.operation, payload.extra);
    Err(UnityCatalogError::NotImplemented {
        message:
            "POST /temporary-path-credentials is scaffolded in preview; implementation is pending."
                .to_string(),
    })
}
