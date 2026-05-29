//! Unity Catalog facade smoke coverage.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use arco_api::config::{Config, Posture};
use arco_api::server::ServerBuilder;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt as _;

fn uc_enabled_config() -> Config {
    let mut config = Config::default();
    config.debug = true;
    config.posture = Posture::Dev;
    config.unity_catalog.enabled = true;
    config
}

#[tokio::test]
async fn mount_path_auth_and_fallback_semantics() {
    let server = ServerBuilder::new().config(uc_enabled_config()).build();
    let router = server.test_router();

    let openapi = Request::builder()
        .uri("/api/2.1/unity-catalog/openapi.json")
        .body(Body::empty())
        .expect("request");
    let openapi_response = router.clone().oneshot(openapi).await.expect("response");
    assert_eq!(openapi_response.status(), StatusCode::OK);

    let unauthorized_known = Request::builder()
        .method("POST")
        .uri("/api/2.1/unity-catalog/catalogs")
        .body(Body::empty())
        .expect("request");
    let unauthorized_response = router
        .clone()
        .oneshot(unauthorized_known)
        .await
        .expect("response");
    assert_eq!(unauthorized_response.status(), StatusCode::UNAUTHORIZED);

    let known_path = Request::builder()
        .method("POST")
        .uri("/api/2.1/unity-catalog/catalogs")
        .header("content-type", "application/json")
        .header("X-Tenant-Id", "acme")
        .header("X-Workspace-Id", "analytics")
        .body(Body::from(r#"{"name":"smoke_catalog"}"#))
        .expect("request");
    let known_response = router.clone().oneshot(known_path).await.expect("response");
    assert_eq!(known_response.status(), StatusCode::OK);

    let planned_path = Request::builder()
        .method("GET")
        .uri("/api/2.1/unity-catalog/volumes/main.default.raw")
        .header("Authorization", "Bearer dev-token")
        .header("X-Tenant-Id", "acme")
        .header("X-Workspace-Id", "analytics")
        .body(Body::empty())
        .expect("request");
    let planned_response = router
        .clone()
        .oneshot(planned_path)
        .await
        .expect("response");
    assert_eq!(planned_response.status(), StatusCode::NOT_IMPLEMENTED);
    let planned_body = axum::body::to_bytes(planned_response.into_body(), usize::MAX)
        .await
        .expect("body");
    let planned_payload: serde_json::Value = serde_json::from_slice(&planned_body).expect("json");
    assert_eq!(planned_payload["error"]["error_code"], "NOT_SUPPORTED");

    let unknown_path = Request::builder()
        .method("GET")
        .uri("/api/2.1/unity-catalog/not-a-real-route")
        .header("X-Tenant-Id", "acme")
        .header("X-Workspace-Id", "analytics")
        .body(Body::empty())
        .expect("request");
    let unknown_response = router.oneshot(unknown_path).await.expect("response");
    assert_eq!(unknown_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn custom_mount_prefix_is_respected() {
    let mut config = uc_enabled_config();
    config.unity_catalog.mount_prefix = "/uc".to_string();
    let server = ServerBuilder::new().config(config).build();
    let router = server.test_router();

    let mounted = Request::builder()
        .uri("/uc/openapi.json")
        .body(Body::empty())
        .expect("request");
    let mounted_response = router.clone().oneshot(mounted).await.expect("response");
    assert_eq!(mounted_response.status(), StatusCode::OK);

    let unknown_custom_mount = Request::builder()
        .method("GET")
        .uri("/uc/not-a-real-route")
        .header("X-Tenant-Id", "acme")
        .header("X-Workspace-Id", "analytics")
        .body(Body::empty())
        .expect("request");
    let unknown_custom_mount_response = router
        .clone()
        .oneshot(unknown_custom_mount)
        .await
        .expect("response");
    assert_eq!(
        unknown_custom_mount_response.status(),
        StatusCode::NOT_FOUND
    );

    let default_mount = Request::builder()
        .uri("/api/2.1/unity-catalog/openapi.json")
        .body(Body::empty())
        .expect("request");
    let default_mount_response = router.oneshot(default_mount).await.expect("response");
    assert_eq!(default_mount_response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn custom_mount_known_uc_gaps_return_structured_501() {
    let mut config = uc_enabled_config();
    config.unity_catalog.mount_prefix = "/uc".to_string();
    let server = ServerBuilder::new().config(config).build();
    let router = server.test_router();

    let response = router
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/uc/volumes/main.default.raw")
                .header("Authorization", "Bearer dev-token")
                .header("X-Tenant-Id", "acme")
                .header("X-Workspace-Id", "analytics")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(payload["error"]["error_code"], "NOT_SUPPORTED");
    let message = payload["error"]["message"].as_str().expect("message");
    assert!(message.contains("GET /uc/volumes/main.default.raw"));
    assert!(message.contains("support-level=planned"));
    assert!(message.contains("route-group=Volumes"));
}
