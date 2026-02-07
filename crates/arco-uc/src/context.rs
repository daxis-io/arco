//! Request context extraction for Unity Catalog facade handlers.

use std::sync::Arc;

use axum::body::Body;
use axum::http::header::HeaderName;
use axum::http::{HeaderMap, HeaderValue, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use arco_core::ScopedStorage;
use arco_core::storage::StorageBackend;

use crate::error::UnityCatalogError;

/// Header name for request IDs.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Per-request context derived from auth and headers.
#[derive(Debug, Clone)]
pub struct UnityCatalogRequestContext {
    /// Tenant identifier.
    pub tenant: String,
    /// Workspace identifier.
    pub workspace: String,
    /// Optional user identifier.
    pub user_id: Option<String>,
    /// Principal groups.
    pub groups: Vec<String>,
    /// Request ID for tracing/correlation.
    pub request_id: String,
    /// Optional idempotency key (safe retries).
    pub idempotency_key: Option<String>,
}

impl UnityCatalogRequestContext {
    /// Creates tenant/workspace scoped storage for this request.
    ///
    /// # Errors
    ///
    /// Returns an error if tenant/workspace identifiers are invalid.
    pub fn scoped_storage(
        &self,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<ScopedStorage, UnityCatalogError> {
        ScopedStorage::new(backend, self.tenant.as_str(), self.workspace.as_str()).map_err(|err| {
            UnityCatalogError::BadRequest {
                message: err.to_string(),
            }
        })
    }

    fn from_headers(headers: &HeaderMap) -> Result<Self, UnityCatalogError> {
        let request_id =
            request_id_from_headers(headers).unwrap_or_else(|| ulid::Ulid::new().to_string());
        let tenant = header_string(headers, "X-Tenant-Id").ok_or_else(|| {
            UnityCatalogError::Unauthorized {
                message: "missing X-Tenant-Id header".to_string(),
            }
        })?;
        let workspace = header_string(headers, "X-Workspace-Id").ok_or_else(|| {
            UnityCatalogError::Unauthorized {
                message: "missing X-Workspace-Id header".to_string(),
            }
        })?;

        Ok(Self {
            tenant,
            workspace,
            user_id: header_string(headers, "X-User-Id")
                .or_else(|| header_string(headers, "X-User-ID")),
            groups: groups_from_headers(headers),
            request_id,
            idempotency_key: header_string(headers, "Idempotency-Key"),
        })
    }
}

fn request_id_from_headers(headers: &HeaderMap) -> Option<String> {
    header_string(headers, "X-Request-Id").or_else(|| header_string(headers, "X-Request-ID"))
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).and_then(header_value_to_string)
}

fn groups_from_headers(headers: &HeaderMap) -> Vec<String> {
    header_string(headers, "X-Groups")
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|group| !group.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn header_value_to_string(value: &HeaderValue) -> Option<String> {
    value.to_str().ok().map(str::to_string)
}

fn add_request_id_header(response: &mut Response, request_id: &str) {
    if let Ok(value) = HeaderValue::from_str(request_id) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
    }
}

fn unity_catalog_public_path(path: &str) -> bool {
    path.ends_with("/openapi.json")
}

/// Middleware that injects a request context and echoes the request ID.
pub async fn context_middleware(req: Request<Body>, next: Next) -> Response {
    if let Some(ctx) = req
        .extensions()
        .get::<UnityCatalogRequestContext>()
        .cloned()
    {
        let request_id = ctx.request_id.clone();
        let mut response = next.run(req).await;
        add_request_id_header(&mut response, &request_id);
        return response;
    }

    let (mut parts, body) = req.into_parts();
    let request_id =
        request_id_from_headers(&parts.headers).unwrap_or_else(|| ulid::Ulid::new().to_string());

    if unity_catalog_public_path(parts.uri.path()) {
        let mut response = next.run(Request::from_parts(parts, body)).await;
        add_request_id_header(&mut response, &request_id);
        return response;
    }

    let ctx = match UnityCatalogRequestContext::from_headers(&parts.headers) {
        Ok(ctx) => ctx,
        Err(err) => {
            let mut response = err.into_response();
            add_request_id_header(&mut response, &request_id);
            return response;
        }
    };

    parts.extensions.insert(ctx.clone());
    let mut response = next.run(Request::from_parts(parts, body)).await;
    add_request_id_header(&mut response, &ctx.request_id);
    response
}
