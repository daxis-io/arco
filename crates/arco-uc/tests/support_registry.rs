//! Contract tests for the Unity Catalog support registry.

use std::collections::BTreeSet;
use std::sync::Arc;

use arco_core::storage::MemoryBackend;
use arco_uc::support::{SupportLevel, operation_support, registry_entries};
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

#[tokio::test]
async fn known_unsupported_permission_mutation_returns_structured_501() {
    let response = request("PATCH", "/permissions/table/main.default.orders").await;

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let payload = json_body(response).await;
    assert_eq!(payload["error"]["error_code"], "NOT_SUPPORTED");
    let message = payload["error"]["message"].as_str().expect("message");
    assert!(message.contains("PATCH /permissions/table/main.default.orders"));
    assert!(message.contains("known-unsupported"));
    assert!(message.contains("Grants"));
}

#[tokio::test]
async fn planned_pinned_uc_operation_returns_structured_501_instead_of_404() {
    let response = request("GET", "/volumes/main.default.raw").await;

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let payload = json_body(response).await;
    assert_eq!(payload["error"]["error_code"], "NOT_SUPPORTED");
    let message = payload["error"]["message"].as_str().expect("message");
    assert!(message.contains("GET /volumes/main.default.raw"));
    assert!(message.contains("planned"));
    assert!(message.contains("Volumes"));
}

#[tokio::test]
async fn unmounted_pinned_credentials_route_returns_structured_501() {
    let response = request("GET", "/credentials").await;

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    let payload = json_body(response).await;
    assert_eq!(payload["error"]["error_code"], "NOT_SUPPORTED");
    let message = payload["error"]["message"].as_str().expect("message");
    assert!(message.contains("GET /credentials"));
    assert!(message.contains("known-unsupported"));
    assert!(message.contains("/storage-credentials"));
}

#[tokio::test]
async fn unknown_non_uc_route_remains_404() {
    let response = request("GET", "/definitely-not-in-spec").await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload = json_body(response).await;
    assert_eq!(payload["error"]["error_code"], "NOT_FOUND");
}

#[test]
fn registry_classifies_current_support_boundary() {
    assert_eq!(
        operation_support("GET", "/catalogs")
            .expect("GET /catalogs")
            .operation
            .support_level,
        SupportLevel::Implemented
    );
    assert_eq!(
        operation_support("POST", "/temporary-table-credentials")
            .expect("temporary table credentials")
            .operation
            .support_level,
        SupportLevel::CompatiblePartial
    );
    assert_eq!(
        operation_support("PATCH", "/permissions/table/main.default.orders")
            .expect("PATCH permissions")
            .operation
            .support_level,
        SupportLevel::KnownUnsupported
    );
    assert_eq!(
        operation_support("GET", "/volumes/main.default.raw")
            .expect("GET volume")
            .operation
            .support_level,
        SupportLevel::Planned
    );
    assert_eq!(
        operation_support("GET", "/api/2.1/unity-catalog/volumes/main.default.raw")
            .expect("mounted GET volume")
            .operation
            .support_level,
        SupportLevel::Planned
    );
}

#[test]
fn mounted_uc_routes_have_support_registry_metadata() {
    let mounted_routes = [
        ("GET", "/catalogs"),
        ("POST", "/catalogs"),
        ("GET", "/catalogs/main"),
        ("PATCH", "/catalogs/main"),
        ("DELETE", "/catalogs/main"),
        ("GET", "/schemas"),
        ("POST", "/schemas"),
        ("GET", "/schemas/main.default"),
        ("PATCH", "/schemas/main.default"),
        ("DELETE", "/schemas/main.default"),
        ("GET", "/tables"),
        ("POST", "/tables"),
        ("GET", "/tables/main.default.orders"),
        ("DELETE", "/tables/main.default.orders"),
        ("GET", "/permissions/table/main.default.orders"),
        ("PATCH", "/permissions/table/main.default.orders"),
        ("GET", "/storage-credentials"),
        ("POST", "/storage-credentials"),
        ("GET", "/storage-credentials/cred_01"),
        ("GET", "/external-locations"),
        ("POST", "/external-locations"),
        ("GET", "/external-locations/orders"),
        ("POST", "/temporary-model-version-credentials"),
        ("POST", "/temporary-table-credentials"),
        ("POST", "/temporary-volume-credentials"),
        ("POST", "/temporary-path-credentials"),
        ("GET", "/delta/preview/commits"),
        ("POST", "/delta/preview/commits"),
    ];

    for (method, path) in mounted_routes {
        assert!(
            operation_support(method, path).is_some(),
            "mounted route missing support metadata: {method} {path}"
        );
    }
}

#[test]
fn registry_entries_are_backed_by_uc_fixture_or_marked_arco_native() {
    let pinned_operations = pinned_uc_operations();
    for operation in registry_entries() {
        assert!(
            operation.arco_native
                || pinned_operations.contains(&(
                    operation.method.to_string(),
                    operation.path_template.to_string()
                )),
            "{} {} must map to the pinned UC fixture or be explicitly Arco-native",
            operation.method,
            operation.path_template
        );
    }
}

#[test]
fn every_pinned_uc_operation_has_explicit_registry_classification() {
    let missing = pinned_uc_operations()
        .into_iter()
        .filter(|(method, path)| operation_support(method, path).is_none())
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "pinned UC operations missing registry entries: {missing:?}"
    );
}

fn pinned_uc_operations() -> BTreeSet<(String, String)> {
    let yaml = include_str!("fixtures/unitycatalog-openapi.yaml");
    let parsed_yaml: serde_yaml::Value = serde_yaml::from_str(yaml).expect("parse UC fixture");
    let parsed_json = serde_json::to_value(parsed_yaml).expect("fixture as json");
    let paths = parsed_json
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .expect("paths object");
    let methods = [
        "get", "post", "put", "patch", "delete", "head", "options", "trace",
    ];
    let mut operations = BTreeSet::new();

    for (path, path_item) in paths {
        let Some(path_item) = path_item.as_object() else {
            continue;
        };
        for method in methods {
            if path_item.contains_key(method) {
                operations.insert((method.to_ascii_uppercase(), path.clone()));
            }
        }
    }
    operations
}

async fn request(method: &str, uri: &str) -> axum::response::Response {
    let state = UnityCatalogState::new(Arc::new(MemoryBackend::new()));
    let app = unity_catalog_router(state);

    app.oneshot(
        Request::builder()
            .method(method)
            .uri(uri)
            .header("X-Tenant-Id", "tenant1")
            .header("X-Workspace-Id", "workspace1")
            .body(Body::empty())
            .expect("request"),
    )
    .await
    .expect("response")
}

async fn json_body(response: axum::response::Response) -> serde_json::Value {
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body bytes");
    serde_json::from_slice(&body).expect("json payload")
}
