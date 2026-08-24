//! Behavior tests for the Delta catalog protocol configuration route
//! (`GET /delta/v1/config`).

// Test-target lint scope (#331): tests and their helpers signal failure by
// panicking. clippy.toml scopes the restriction lints out of #[test] fns;
// this header extends the same policy to this file's shared helpers.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use arco_catalog::{CatalogWriter, Tier1Compactor, WriteOptions};
use arco_core::ScopedStorage;
use arco_core::storage::MemoryBackend;
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

async fn seeded_app() -> axum::Router {
    let backend = Arc::new(MemoryBackend::new());
    let scoped =
        ScopedStorage::new(backend.clone(), "tenant1", "workspace1").expect("scoped storage");

    let writer = CatalogWriter::new(scoped.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(scoped.clone())))
        // Tier-1 DDL validates fencing token expiration; keep the TTL comfortably
        // above any in-process compaction/IO jitter to avoid flakiness.
        .with_lock_policy(std::time::Duration::from_secs(5), 3);
    writer.initialize().await.expect("initialize");
    writer
        .create_catalog(
            "analytics",
            Some("Analytics catalog"),
            WriteOptions::default(),
        )
        .await
        .expect("create catalog");

    unity_catalog_router(UnityCatalogState::new(backend))
}

async fn get_config(app: axum::Router, query: &str) -> (StatusCode, serde_json::Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/delta/v1/config{query}"))
                .header("X-Tenant-Id", "tenant1")
                .header("X-Workspace-Id", "workspace1")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    (status, serde_json::from_slice(&body).expect("json payload"))
}

#[tokio::test]
async fn config_returns_negotiated_protocol_and_endpoints() {
    let app = seeded_app().await;
    let (status, payload) = get_config(app, "?catalog=analytics&protocol-versions=1.0").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["protocol-version"], "1.0");
    let endpoints = payload["endpoints"].as_array().expect("endpoints array");

    for endpoint in endpoints {
        let endpoint = endpoint.as_str().expect("endpoint string");

        let parts: Vec<&str> = endpoint.split(' ').collect();
        assert_eq!(
            parts.len(),
            2,
            "endpoint signature must be exactly 'METHOD /v1/path'"
        );

        let (method, _path) = (parts[0], parts[1]);
        assert_eq!(
            method,
            method.to_uppercase(),
            "http method must be uppercase"
        );
        assert!(
            method.parse::<http::Method>().is_ok(),
            "http method must be valid"
        );
        // When UC Delta API endpoints are supported, assert that
        // the path is valid for the negotiated protocol version.
    }
}

#[tokio::test]
async fn config_requires_catalog_query_parameter() {
    let app = seeded_app().await;
    let (status, payload) = get_config(app, "?protocol-versions=1.0").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = payload["error"]["message"].as_str().expect("error message");
    assert!(message.contains("catalog"));
}

#[tokio::test]
async fn config_returns_not_found_for_unknown_catalog() {
    let app = seeded_app().await;
    let (status, payload) = get_config(app, "?catalog=missing&protocol-versions=1.0").await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    let message = payload["error"]["message"].as_str().expect("error message");
    assert!(message.contains("catalog not found"));
}

#[tokio::test]
async fn config_does_not_require_protocol_versions_query_parameter() {
    // Deliberate deviation from the pinned delta spec: `protocol-versions` is
    // required there (missing -> 400), but the current handler ignores it and
    // always negotiates 1.0.
    let app = seeded_app().await;
    let (status, payload) = get_config(app, "?catalog=analytics").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["protocol-version"], "1.0");
}

#[tokio::test]
async fn config_ignores_declared_protocol_versions_for_now() {
    // Deliberate deviation: the client declared 2.0-2.1 and 3.0-3.2, which do
    // not cover the server's 1.0; the pinned spec would require a 400 naming
    // the supported version. The handler hardcodes 1.0, for now.
    let app = seeded_app().await;
    let (status, payload) = get_config(app, "?catalog=analytics&protocol-versions=2.1,3.2").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["protocol-version"], "1.0");
}
