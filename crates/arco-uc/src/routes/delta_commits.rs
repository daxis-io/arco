//! Delta commit coordinator endpoints for the UC facade.

use axum::Router;
use axum::extract::OriginalUri;
use axum::http::Method;
use axum::routing::get;

use crate::error::UnityCatalogError;
use crate::error::UnityCatalogErrorResponse;
use crate::state::UnityCatalogState;

/// Delta commit route group.
pub fn routes() -> Router<UnityCatalogState> {
    Router::new().route(
        "/delta/preview/commits",
        get(list_unbackfilled_commits).post(register_commit),
    )
}

/// `GET /delta/preview/commits` (known UC operation; currently unsupported).
#[utoipa::path(
    get,
    path = "/delta/preview/commits",
    tag = "DeltaCommits",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn list_unbackfilled_commits(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}

/// `POST /delta/preview/commits` (known UC operation; currently unsupported).
#[utoipa::path(
    post,
    path = "/delta/preview/commits",
    tag = "DeltaCommits",
    responses(
        (status = 501, description = "Operation not supported", body = UnityCatalogErrorResponse),
    )
)]
pub async fn register_commit(method: Method, uri: OriginalUri) -> UnityCatalogError {
    super::common::known_but_unsupported(method, uri).await
}
