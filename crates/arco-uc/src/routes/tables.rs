//! Table endpoints for the UC facade.

use axum::Json;
use axum::Router;
use axum::extract::{Extension, OriginalUri, Path, State};
use axum::http::Method;
use axum::http::StatusCode;
use axum::routing::{get, post};

use serde_json::json;

use crate::context::UnityCatalogRequestContext;
use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::error::UnityCatalogResult;
use crate::state::UnityCatalogState;

/// Table route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new()
        .route("/tables", post(create_table).get(list_tables))
        .route("/tables/:full_name", get(get_table).delete(delete_table))
}

/// `POST /tables` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/tables",
    tag = "Tables",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn create_table(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}

/// `GET /tables` (known UC operation; currently unsupported).
#[utoipa::path(
    get,
    path = "/tables",
    tag = "Tables",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn list_tables(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}

/// `GET /tables/{full_name}` (Scope A).
///
/// # Errors
///
/// Returns an error if `full_name` is malformed or the referenced table cannot
/// be read from storage.
#[utoipa::path(
    get,
    path = "/tables/{full_name}",
    tag = "Tables",
    params(
        ("full_name" = String, Path, description = "Table full name"),
    ),
    responses(
        (status = 200, description = "Get table"),
    )
)]
pub async fn get_table(
    State(state): State<UnityCatalogState>,
    Extension(ctx): Extension<UnityCatalogRequestContext>,
    Path(full_name): Path<String>,
) -> UnityCatalogResult<(StatusCode, Json<serde_json::Value>)> {
    let mut iter = full_name.split('.');
    let catalog = iter.next();
    let schema = iter.next();
    let table = iter.next();
    let extra = iter.next();

    let (Some(catalog), Some(schema), Some(table), None) = (catalog, schema, table, extra) else {
        return Err(UnityCatalogError::BadRequest {
            message: format!("invalid table full name: {full_name}"),
        });
    };

    let reader = super::common::catalog_reader(&state, &ctx)?;
    let table = reader
        .get_table_in_schema(catalog, schema, table)
        .await
        .map_err(super::common::map_catalog_error)?
        .ok_or_else(|| UnityCatalogError::NotFound {
            message: format!("table not found: {full_name}"),
        })?;

    let payload = json!({
        "name": table.name,
        "table_id": table.id,
        "storage_location": table.location,
        "data_source_format": table.format,
        "comment": table.description,
    });

    Ok((StatusCode::OK, Json(payload)))
}

/// `DELETE /tables/{full_name}` (known UC operation; currently unsupported).
#[utoipa::path(
    delete,
    path = "/tables/{full_name}",
    tag = "Tables",
    params(
        ("full_name" = String, Path, description = "Table full name"),
    ),
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn delete_table(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(&method, &uri)
}
