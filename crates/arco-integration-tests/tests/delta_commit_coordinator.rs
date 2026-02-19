//! Delta commit coordination integration tests.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arco_api::config::{Config, Posture};
use arco_api::routes;
use arco_api::server::AppState;
use arco_core::ScopedStorage;
use arco_core::storage::WritePrecondition;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use bytes::Bytes;
use chrono::Utc;
use tower::ServiceExt as _;
use uuid::Uuid;

#[derive(Debug, serde::Serialize)]
struct StageCommitRequest {
    payload: String,
}

#[derive(Debug, serde::Deserialize)]
struct StageCommitResponse {
    staged_path: String,
    staged_version: String,
}

#[derive(Debug, serde::Serialize)]
struct CommitRequest<'a> {
    read_version: i64,
    staged_path: &'a str,
    staged_version: &'a str,
}

#[derive(Debug, serde::Deserialize)]
struct CommitResponse {
    version: i64,
    delta_log_path: String,
}

#[derive(Debug, serde::Deserialize)]
struct CreatedTableResponse {
    id: String,
    format: String,
}

fn make_test_state() -> Arc<AppState> {
    let config = Config {
        debug: true,
        posture: Posture::Dev,
        ..Config::default()
    };
    Arc::new(AppState::with_memory_storage(config))
}

async fn create_catalog_and_schema(
    app: &axum::Router,
    tenant: &str,
    workspace: &str,
    catalog: &str,
    schema: &str,
) {
    let create_catalog = Request::builder()
        .method("POST")
        .uri("/catalogs")
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({ "name": catalog })).unwrap(),
        ))
        .unwrap();
    let catalog_resp = app.clone().oneshot(create_catalog).await.unwrap();
    assert_eq!(catalog_resp.status(), StatusCode::CREATED);

    let create_schema = Request::builder()
        .method("POST")
        .uri(format!("/catalogs/{catalog}/schemas"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({ "name": schema })).unwrap(),
        ))
        .unwrap();
    let schema_resp = app.clone().oneshot(create_schema).await.unwrap();
    assert_eq!(schema_resp.status(), StatusCode::CREATED);
}

async fn create_table_in_schema(
    app: &axum::Router,
    tenant: &str,
    workspace: &str,
    catalog: &str,
    schema: &str,
    table_name: &str,
    format: Option<&str>,
    location: &str,
) -> CreatedTableResponse {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/catalogs/{catalog}/schemas/{schema}/tables"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({
                "name": table_name,
                "format": format,
                "columns": [],
                "description": null,
                "location": location,
            }))
            .unwrap(),
        ))
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn delta_commit_writes_delta_log_enforces_occ_and_replays_idempotency() {
    let state = make_test_state();
    let app = routes::api_v1_routes().with_state(Arc::clone(&state));

    let tenant = "acme";
    let workspace = "analytics";
    create_catalog_and_schema(&app, tenant, workspace, "analytics", "sales").await;
    let created = create_table_in_schema(
        &app,
        tenant,
        workspace,
        "analytics",
        "sales",
        "orders_delta",
        Some("delta"),
        "warehouse/sales/orders_delta",
    )
    .await;
    assert_eq!(created.format, "delta");
    let table_id = Uuid::parse_str(&created.id).unwrap();

    let payload_v0 = "{\"commitInfo\":{\"timestamp\":1}}\n".to_string();
    let stage_body = serde_json::to_vec(&StageCommitRequest {
        payload: payload_v0.clone(),
    })
    .unwrap();

    let stage_req = Request::builder()
        .method("POST")
        .uri(format!("/delta/tables/{table_id}/commits/stage"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .body(Body::from(stage_body))
        .unwrap();

    let stage_resp = app.clone().oneshot(stage_req).await.unwrap();
    assert_eq!(stage_resp.status(), StatusCode::OK);
    let stage_bytes = axum::body::to_bytes(stage_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let staged: StageCommitResponse = serde_json::from_slice(&stage_bytes).unwrap();

    let idempotency_key_v0 = Uuid::now_v7().to_string();
    let commit_body = serde_json::to_vec(&CommitRequest {
        read_version: -1,
        staged_path: &staged.staged_path,
        staged_version: &staged.staged_version,
    })
    .unwrap();

    let commit_req = Request::builder()
        .method("POST")
        .uri(format!("/delta/tables/{table_id}/commits"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", idempotency_key_v0.clone())
        .body(Body::from(commit_body.clone()))
        .unwrap();

    let commit_resp = app.clone().oneshot(commit_req).await.unwrap();
    assert_eq!(commit_resp.status(), StatusCode::OK);
    let commit_bytes = axum::body::to_bytes(commit_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let committed: CommitResponse = serde_json::from_slice(&commit_bytes).unwrap();
    assert_eq!(committed.version, 0);
    assert_eq!(
        committed.delta_log_path,
        "warehouse/sales/orders_delta/_delta_log/00000000000000000000.json"
    );

    let backend = state.storage_backend().expect("backend");
    let storage = ScopedStorage::new(backend, tenant, workspace).expect("scoped storage");
    let written = storage.get_raw(&committed.delta_log_path).await.unwrap();
    assert_eq!(written, Bytes::from(payload_v0));

    // OCC: a second commit with stale read_version should conflict.
    let payload_v1 = "{\"commitInfo\":{\"timestamp\":2}}\n".to_string();
    let stage_body_v1 = serde_json::to_vec(&StageCommitRequest {
        payload: payload_v1,
    })
    .unwrap();
    let stage_req_v1 = Request::builder()
        .method("POST")
        .uri(format!("/delta/tables/{table_id}/commits/stage"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .body(Body::from(stage_body_v1))
        .unwrap();
    let stage_resp_v1 = app.clone().oneshot(stage_req_v1).await.unwrap();
    assert_eq!(stage_resp_v1.status(), StatusCode::OK);
    let stage_bytes_v1 = axum::body::to_bytes(stage_resp_v1.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let staged_v1: StageCommitResponse = serde_json::from_slice(&stage_bytes_v1).unwrap();

    let stale_commit_body = serde_json::to_vec(&CommitRequest {
        read_version: -1,
        staged_path: &staged_v1.staged_path,
        staged_version: &staged_v1.staged_version,
    })
    .unwrap();
    let stale_req = Request::builder()
        .method("POST")
        .uri(format!("/delta/tables/{table_id}/commits"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", Uuid::now_v7().to_string())
        .body(Body::from(stale_commit_body))
        .unwrap();
    let stale_resp = app.clone().oneshot(stale_req).await.unwrap();
    assert_eq!(stale_resp.status(), StatusCode::CONFLICT);

    // Idempotency replay: same key + same body returns the cached response.
    let replay_req = Request::builder()
        .method("POST")
        .uri(format!("/delta/tables/{table_id}/commits"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", idempotency_key_v0)
        .body(Body::from(commit_body))
        .unwrap();
    let replay_resp = app.oneshot(replay_req).await.unwrap();
    assert_eq!(replay_resp.status(), StatusCode::OK);
    let replay_bytes = axum::body::to_bytes(replay_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let replayed: CommitResponse = serde_json::from_slice(&replay_bytes).unwrap();
    assert_eq!(replayed.version, 0);
    assert_eq!(
        replayed.delta_log_path,
        "warehouse/sales/orders_delta/_delta_log/00000000000000000000.json"
    );
}

#[tokio::test]
async fn expired_inflight_commit_is_recovered_but_not_acked_for_unrelated_idempotency_key() {
    let state = make_test_state();
    let app = routes::api_v1_routes().with_state(Arc::clone(&state));

    let tenant = "acme";
    let workspace = "analytics";
    create_catalog_and_schema(&app, tenant, workspace, "analytics", "sales").await;
    let created = create_table_in_schema(
        &app,
        tenant,
        workspace,
        "analytics",
        "sales",
        "recovery_delta",
        Some("delta"),
        "warehouse/sales/recovery_delta",
    )
    .await;
    assert_eq!(created.format, "delta");
    let table_id = Uuid::parse_str(&created.id).unwrap();

    // Stage a payload.
    let payload = "{\"commitInfo\":{\"timestamp\":3}}\n".to_string();
    let stage_body = serde_json::to_vec(&StageCommitRequest {
        payload: payload.clone(),
    })
    .unwrap();
    let stage_req = Request::builder()
        .method("POST")
        .uri(format!("/delta/tables/{table_id}/commits/stage"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .body(Body::from(stage_body))
        .unwrap();
    let stage_resp = app.clone().oneshot(stage_req).await.unwrap();
    assert_eq!(stage_resp.status(), StatusCode::OK);
    let stage_bytes = axum::body::to_bytes(stage_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let staged: StageCommitResponse = serde_json::from_slice(&stage_bytes).unwrap();

    // Seed an expired inflight reservation in the coordinator state.
    let backend = state.storage_backend().expect("backend");
    let storage = ScopedStorage::new(backend, tenant, workspace).expect("scoped storage");

    let now_ms = Utc::now().timestamp_millis();
    let inflight_key = Uuid::now_v7().to_string();
    let state_json = serde_json::to_vec(&arco_delta::DeltaCoordinatorState {
        latest_version: -1,
        inflight: Some(arco_delta::InflightCommit {
            commit_id: inflight_key.clone(),
            read_version: Some(-1),
            version: 0,
            staged_path: staged.staged_path.clone(),
            staged_version: staged.staged_version.clone(),
            started_at_ms: now_ms - 30_000,
            expires_at_ms: now_ms - 10_000,
        }),
    })
    .unwrap();

    storage
        .put_raw(
            &format!("delta/coordinator/{table_id}.json"),
            Bytes::from(state_json),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();

    // A commit request with a different Idempotency-Key should recover the inflight commit,
    // but still return a conflict because its `read_version` is now stale.
    let commit_body = serde_json::to_vec(&CommitRequest {
        read_version: -1,
        staged_path: &staged.staged_path,
        staged_version: &staged.staged_version,
    })
    .unwrap();
    let commit_req = Request::builder()
        .method("POST")
        .uri(format!("/delta/tables/{table_id}/commits"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", Uuid::now_v7().to_string())
        .body(Body::from(commit_body))
        .unwrap();

    let commit_resp = app.clone().oneshot(commit_req).await.unwrap();
    assert_eq!(commit_resp.status(), StatusCode::CONFLICT);

    // Delta log file exists and matches the staged payload.
    let delta_path = "warehouse/sales/recovery_delta/_delta_log/00000000000000000000.json";
    let written = storage.get_raw(&delta_path).await.unwrap();
    assert_eq!(written, Bytes::from(payload));

    // Coordinator state is finalized.
    let bytes = storage
        .get_raw(&format!("delta/coordinator/{table_id}.json"))
        .await
        .unwrap();
    let state: arco_delta::DeltaCoordinatorState = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(state.latest_version, 0);
    assert!(state.inflight.is_none());

    // The original inflight Idempotency-Key should replay the committed version.
    let replay_req = Request::builder()
        .method("POST")
        .uri(format!("/delta/tables/{table_id}/commits"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("Idempotency-Key", inflight_key)
        .body(Body::from(
            serde_json::to_vec(&CommitRequest {
                read_version: -1,
                staged_path: &staged.staged_path,
                staged_version: &staged.staged_version,
            })
            .unwrap(),
        ))
        .unwrap();
    let replay_resp = app.oneshot(replay_req).await.unwrap();
    assert_eq!(replay_resp.status(), StatusCode::OK);
    let replay_body = axum::body::to_bytes(replay_resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let replayed: CommitResponse = serde_json::from_slice(&replay_body).unwrap();
    assert_eq!(replayed.version, 0);
    assert_eq!(replayed.delta_log_path, delta_path);
}

#[tokio::test]
async fn delta_commit_stage_rejects_missing_table() {
    let state = make_test_state();
    let app = routes::api_v1_routes().with_state(Arc::clone(&state));

    let stage_req = Request::builder()
        .method("POST")
        .uri(format!("/delta/tables/{}/commits/stage", Uuid::now_v7()))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", "acme")
        .header("X-Workspace-Id", "analytics")
        .body(Body::from(
            serde_json::to_vec(&StageCommitRequest {
                payload: "{\"commitInfo\":{\"timestamp\":1}}\n".to_string(),
            })
            .unwrap(),
        ))
        .unwrap();

    let response = app.oneshot(stage_req).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn delta_commit_stage_rejects_non_delta_table() {
    let state = make_test_state();
    let app = routes::api_v1_routes().with_state(Arc::clone(&state));

    let tenant = "acme";
    let workspace = "analytics";
    create_catalog_and_schema(&app, tenant, workspace, "analytics", "sales").await;
    let created = create_table_in_schema(
        &app,
        tenant,
        workspace,
        "analytics",
        "sales",
        "orders_iceberg",
        Some("iceberg"),
        "warehouse/sales/orders_iceberg",
    )
    .await;
    assert_eq!(created.format, "iceberg");
    let table_id = Uuid::parse_str(&created.id).unwrap();

    let stage_req = Request::builder()
        .method("POST")
        .uri(format!("/delta/tables/{table_id}/commits/stage"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .body(Body::from(
            serde_json::to_vec(&StageCommitRequest {
                payload: "{\"commitInfo\":{\"timestamp\":1}}\n".to_string(),
            })
            .unwrap(),
        ))
        .unwrap();

    let response = app.oneshot(stage_req).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
}
