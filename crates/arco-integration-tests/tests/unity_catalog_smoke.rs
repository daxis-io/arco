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
        .header("X-Tenant-Id", "acme")
        .header("X-Workspace-Id", "analytics")
        .body(Body::empty())
        .expect("request");
    let known_response = router.clone().oneshot(known_path).await.expect("response");
    assert_eq!(known_response.status(), StatusCode::NOT_IMPLEMENTED);

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

    let default_mount = Request::builder()
        .uri("/api/2.1/unity-catalog/openapi.json")
        .body(Body::empty())
        .expect("request");
    let default_mount_response = router.oneshot(default_mount).await.expect("response");
    assert_eq!(default_mount_response.status(), StatusCode::NOT_FOUND);
}
