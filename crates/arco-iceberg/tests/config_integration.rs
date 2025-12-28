//! Integration tests for the /v1/config endpoint.

use arco_iceberg::router::iceberg_router;
use arco_iceberg::state::IcebergState;
use arco_iceberg::types::ConfigResponse;
use arco_core::storage::MemoryBackend;
use axum::body::Body;
use axum::http::Request;
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn test_config_endpoint_includes_request_id_and_fields() {
    let storage = Arc::new(MemoryBackend::new());
    let state = IcebergState::new(storage);
    let app = iceberg_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/config")
                .body(Body::empty())
                .expect("request build failed"),
        )
        .await
        .expect("request failed");

    assert!(response.headers().contains_key("x-request-id"));

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body read failed");
    let config: ConfigResponse = serde_json::from_slice(&body).expect("json parse failed");

    assert_eq!(config.overrides.get("prefix"), Some(&"arco".to_string()));
    assert_eq!(
        config.overrides.get("namespace-separator"),
        Some(&"%1F".to_string())
    );
    let endpoints = config.endpoints.expect("endpoints should be present");
    assert!(endpoints.contains(&"GET /v1/config".to_string()));
}
