//! Request context extraction for Iceberg REST handlers.

use std::sync::Arc;

use axum::body::Body;
use axum::http::header::HeaderName;
use axum::http::{HeaderMap, HeaderValue, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use arco_core::ScopedStorage;
use arco_core::storage::StorageBackend;

use crate::error::{IcebergError, IcebergResult};

/// Header name for request IDs.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Per-request context derived from headers.
#[derive(Debug, Clone)]
pub struct IcebergRequestContext {
    /// Tenant identifier.
    pub tenant: String,
    /// Workspace identifier.
    pub workspace: String,
    /// Request ID for tracing/correlation.
    pub request_id: String,
    /// Optional idempotency key (safe retries).
    pub idempotency_key: Option<String>,
}

impl IcebergRequestContext {
    /// Creates tenant/workspace scoped storage for this request.
    ///
    /// # Errors
    ///
    /// Returns an error if tenant/workspace identifiers are invalid.
    pub fn scoped_storage(&self, backend: Arc<dyn StorageBackend>) -> IcebergResult<ScopedStorage> {
        ScopedStorage::new(backend, self.tenant.as_str(), self.workspace.as_str()).map_err(|err| {
            IcebergError::BadRequest {
                message: err.to_string(),
                error_type: "BadRequestException",
            }
        })
    }

    fn from_headers(headers: &HeaderMap) -> IcebergResult<Self> {
        let request_id =
            request_id_from_headers(headers).unwrap_or_else(|| ulid::Ulid::new().to_string());
        let tenant =
            header_string(headers, "X-Tenant-Id").ok_or_else(|| IcebergError::Unauthorized {
                message: "missing X-Tenant-Id header".to_string(),
            })?;
        let workspace =
            header_string(headers, "X-Workspace-Id").ok_or_else(|| IcebergError::Unauthorized {
                message: "missing X-Workspace-Id header".to_string(),
            })?;

        Ok(Self {
            tenant,
            workspace,
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

/// Middleware that injects a request context and echoes the request ID.
pub async fn context_middleware(req: Request<Body>, next: Next) -> Response {
    if let Some(ctx) = req.extensions().get::<IcebergRequestContext>().cloned() {
        let request_id = ctx.request_id.clone();
        let mut response = next.run(req).await;
        add_request_id_header(&mut response, &request_id);
        return response;
    }

    let (mut parts, body) = req.into_parts();
    let request_id =
        request_id_from_headers(&parts.headers).unwrap_or_else(|| ulid::Ulid::new().to_string());

    if iceberg_public_path(parts.uri.path()) {
        let mut response = next.run(Request::from_parts(parts, body)).await;
        add_request_id_header(&mut response, &request_id);
        return response;
    }

    let ctx = match IcebergRequestContext::from_headers(&parts.headers) {
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

fn iceberg_public_path(path: &str) -> bool {
    path.ends_with("/v1/config") || path.ends_with("/openapi.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::HeaderValue;

    #[test]
    fn test_context_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Tenant-Id", HeaderValue::from_static("acme"));
        headers.insert("X-Workspace-Id", HeaderValue::from_static("prod"));

        let ctx = IcebergRequestContext::from_headers(&headers).expect("context");
        assert_eq!(ctx.tenant, "acme");
        assert_eq!(ctx.workspace, "prod");
        assert!(ctx.request_id.len() >= 26);
    }

    #[test]
    fn test_missing_tenant_header() {
        let headers = HeaderMap::new();
        let err = IcebergRequestContext::from_headers(&headers).expect_err("missing");
        assert!(matches!(err, IcebergError::Unauthorized { .. }));
    }
}
