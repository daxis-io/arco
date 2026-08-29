//! Behavior tests for the Delta catalog protocol configuration route
//! (`GET /delta/v1/config`).
//!
//! NOTE: Protocol version negotiation is intentionally not tested here;
//! it is covered by separate tests.

// Test-target lint scope (#331): tests and their helpers signal failure by
// panicking. clippy.toml scopes the restriction lints out of #[test] fns;
// this header extends the same policy to this file's shared helpers.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use arco_catalog::{CatalogWriter, Tier1Compactor, WriteOptions};
use arco_core::ScopedStorage;
use arco_core::storage::MemoryBackend;
use arco_uc::{SUPPORTED_UC_DELTA_PROTOCOL_VERSIONS, UnityCatalogState, unity_catalog_router};
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

const TENANT: &str = "tenant1";
const WORKSPACE: &str = "workspace1";
const CATALOG: &str = "analytics";

fn supported_protocol_version() -> Option<String> {
    Some(SUPPORTED_UC_DELTA_PROTOCOL_VERSIONS.first()?.to_string())
}

async fn seeded_app() -> axum::Router {
    let backend = Arc::new(MemoryBackend::new());
    let scoped = ScopedStorage::new(backend.clone(), TENANT, WORKSPACE).expect("scoped storage");

    let writer = CatalogWriter::new(scoped.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(scoped.clone())))
        // Tier-1 DDL validates fencing token expiration; keep the TTL comfortably
        // above any in-process compaction/IO jitter to avoid flakiness.
        .with_lock_policy(std::time::Duration::from_secs(5), 3);
    writer.initialize().await.expect("initialize");
    writer
        .create_catalog(CATALOG, Some("Analytics catalog"), WriteOptions::default())
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
                .header("X-Tenant-Id", TENANT)
                .header("X-Workspace-Id", WORKSPACE)
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
    let supported_version = supported_protocol_version().expect("supported version");
    let (status, payload) = get_config(
        app,
        &format!("?catalog={CATALOG}&protocol-versions={supported_version}"),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(payload["protocol-version"], supported_version);
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
    let supported_version = supported_protocol_version().expect("supported version");
    let (status, payload) =
        get_config(app, &format!("?protocol-versions={supported_version}")).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = payload["error"]["message"].as_str().expect("error message");
    assert!(message.contains("catalog"));
}

#[tokio::test]
async fn config_requires_protocol_versions_query_parameter() {
    let app = seeded_app().await;
    let (status, payload) = get_config(app, &format!("?catalog={CATALOG}")).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = payload["error"]["message"].as_str().expect("error message");
    assert!(message.contains("protocol-versions"));
}

#[tokio::test]
async fn config_returns_bad_request_for_unsupported_version() {
    let app = seeded_app().await;
    let (status, payload) = get_config(
        app,
        &format!("?catalog={CATALOG}&protocol-versions=100.100"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = payload["error"]["message"].as_str().expect("error message");
    assert!(message.contains("no mutually supported protocol version"));
}

#[tokio::test]
async fn config_returns_not_found_for_unknown_catalog() {
    let app = seeded_app().await;
    let supported_version = supported_protocol_version().expect("supported version");
    let (status, payload) = get_config(
        app,
        &format!("?catalog=missing&protocol-versions={supported_version}"),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    let message = payload["error"]["message"].as_str().expect("error message");
    assert!(message.contains("catalog not found"));
}

#[tokio::test]
async fn config_rejects_malformed_protocol_versions() {
    let app = seeded_app().await;
    for versions in ["ab.c", "1", "1.", "1.0.1", "1.0,,2.0", "1.0,"] {
        let (status, payload) = get_config(
            app.clone(),
            &format!("?catalog={CATALOG}&protocol-versions={versions}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let message = payload["error"]["message"].as_str().expect("error message");
        assert!(message.contains("invalid protocol-versions"),);
    }
}
