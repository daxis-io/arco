use std::sync::Arc;

use anyhow::{Context, Result};
use axum::body::Body;
use axum::http::{Method, Request, header};

use arco_api::server::ServerBuilder;
use arco_core::storage::MemoryBackend;

pub fn test_router_with_backend() -> (axum::Router, Arc<MemoryBackend>) {
    let backend = Arc::new(MemoryBackend::new());
    let router = ServerBuilder::new()
        .debug(true)
        .storage_backend(backend.clone())
        .build()
        .test_router();
    (router, backend)
}

pub mod helpers {
    use super::*;

    pub fn make_request(
        method: Method,
        uri: &str,
        body: Option<serde_json::Value>,
    ) -> Result<Request<Body>> {
        make_request_with_scope(method, uri, "test-tenant", "test-workspace", body)
    }

    pub fn make_request_with_scope(
        method: Method,
        uri: &str,
        tenant: &str,
        workspace: &str,
        body: Option<serde_json::Value>,
    ) -> Result<Request<Body>> {
        let body = match body {
            Some(value) => {
                Body::from(serde_json::to_vec(&value).context("serialize request body")?)
            }
            None => Body::empty(),
        };
        Request::builder()
            .method(method)
            .uri(uri)
            .header("X-Tenant-Id", tenant)
            .header("X-Workspace-Id", workspace)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .context("build request")
    }
}
