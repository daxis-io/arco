//! Delta preview commit semantics tests for UC facade coordinator mapping.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arco_core::ScopedStorage;
use arco_core::storage::{MemoryBackend, WritePrecondition};
use arco_uc::{UnityCatalogState, unity_catalog_router};
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

#[derive(Debug)]
struct UcResponse {
    status: StatusCode,
    body: Value,
    request_id: Option<String>,
}

struct Harness {
    router: Router,
    storage: ScopedStorage,
}

fn make_harness() -> Harness {
    let backend = Arc::new(MemoryBackend::new());
    let state = UnityCatalogState::new(backend.clone());
    let router = unity_catalog_router(state);
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1").expect("scoped storage");
    Harness { router, storage }
}

async fn uc_request(
    router: &Router,
    method: Method,
    uri: &str,
    body: Value,
    idempotency_key: Option<&str>,
    request_id: Option<&str>,
) -> Result<UcResponse, String> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", "tenant1")
        .header("X-Workspace-Id", "workspace1");

    if let Some(idempotency_key) = idempotency_key {
        builder = builder.header("Idempotency-Key", idempotency_key);
    }
    if let Some(request_id) = request_id {
        builder = builder.header("X-Request-Id", request_id);
    }

    let req = builder
        .body(Body::from(body.to_string()))
        .map_err(|err| format!("build request: {err}"))?;

    let response = router
        .clone()
        .oneshot(req)
        .await
        .map_err(|err| format!("route request: {err}"))?;

    let status = response.status();
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .map_err(|err| format!("read response body: {err}"))?;
    let body = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).map_err(|err| format!("parse response body: {err}"))?
    };

    Ok(UcResponse {
        status,
        body,
        request_id,
    })
}

fn commit_info(version: i64) -> Value {
    let file_name = format!("{version:020}.json");
    json!({
        "version": version,
        "timestamp": 1,
        "file_name": file_name,
        "file_size": 42,
        "file_modification_timestamp": 1
    })
}

#[tokio::test]
async fn idempotency_replay_is_deduplicated_in_get_commits() -> Result<(), String> {
    let harness = make_harness();
    let table_id = Uuid::now_v7();
    let key = Uuid::now_v7().to_string();

    let commit_body = json!({
        "table_id": table_id,
        "table_uri": "gs://bucket/path",
        "commit_info": commit_info(0)
    });

    let first = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        commit_body.clone(),
        Some(&key),
        Some("req-uc-1"),
    )
    .await?;
    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(first.request_id.as_deref(), Some("req-uc-1"));

    let replay = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        commit_body,
        Some(&key),
        Some("req-uc-2"),
    )
    .await?;
    assert_eq!(replay.status, StatusCode::OK);

    let listed = uc_request(
        &harness.router,
        Method::GET,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path",
            "start_version": 0
        }),
        None,
        Some("req-uc-3"),
    )
    .await?;
    assert_eq!(listed.status, StatusCode::OK);
    assert_eq!(listed.request_id.as_deref(), Some("req-uc-3"));
    assert_eq!(
        listed
            .body
            .get("latest_table_version")
            .and_then(Value::as_i64),
        Some(0)
    );
    assert_eq!(
        listed
            .body
            .get("commits")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );

    Ok(())
}

#[tokio::test]
async fn stale_read_rejected_as_conflict() -> Result<(), String> {
    let harness = make_harness();
    let table_id = Uuid::now_v7();

    let first = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path",
            "commit_info": commit_info(0)
        }),
        Some(&Uuid::now_v7().to_string()),
        Some("req-uc-stale-1"),
    )
    .await?;
    assert_eq!(first.status, StatusCode::OK);

    let stale = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path",
            "commit_info": commit_info(0)
        }),
        Some(&Uuid::now_v7().to_string()),
        Some("req-uc-stale-2"),
    )
    .await?;

    assert_eq!(stale.status, StatusCode::CONFLICT);
    assert_eq!(
        stale
            .body
            .pointer("/error/error_code")
            .and_then(Value::as_str),
        Some("CONFLICT")
    );
    assert_eq!(stale.request_id.as_deref(), Some("req-uc-stale-2"));

    Ok(())
}

#[tokio::test]
async fn invalid_idempotency_key_is_rejected_without_staging_side_effects() -> Result<(), String> {
    let harness = make_harness();
    let table_id = Uuid::now_v7();

    let response = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path",
            "commit_info": commit_info(0)
        }),
        Some("not-a-uuid"),
        Some("req-uc-bad-idempotency"),
    )
    .await?;

    assert_eq!(response.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .body
            .pointer("/error/error_code")
            .and_then(Value::as_str),
        Some("BAD_REQUEST")
    );

    let staged_paths = harness
        .storage
        .list(&format!("delta/staging/{table_id}/"))
        .await
        .map_err(|err| format!("list staged payloads: {err}"))?;
    assert!(
        staged_paths.is_empty(),
        "invalid idempotency key should not write staged payloads"
    );

    Ok(())
}

#[tokio::test]
async fn table_uri_mismatch_is_rejected_for_get_and_backfill() -> Result<(), String> {
    let harness = make_harness();
    let table_id = Uuid::now_v7();

    let commit = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path-a",
            "commit_info": commit_info(0)
        }),
        Some(&Uuid::now_v7().to_string()),
        Some("req-uc-uri-commit"),
    )
    .await?;
    assert_eq!(commit.status, StatusCode::OK);

    let get_mismatch = uc_request(
        &harness.router,
        Method::GET,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path-b",
            "start_version": 0
        }),
        None,
        Some("req-uc-uri-get"),
    )
    .await?;
    assert_eq!(get_mismatch.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        get_mismatch
            .body
            .pointer("/error/error_code")
            .and_then(Value::as_str),
        Some("BAD_REQUEST")
    );

    let backfill_mismatch = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path-b",
            "latest_backfilled_version": 0
        }),
        None,
        Some("req-uc-uri-backfill"),
    )
    .await?;
    assert_eq!(backfill_mismatch.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        backfill_mismatch
            .body
            .pointer("/error/error_code")
            .and_then(Value::as_str),
        Some("BAD_REQUEST")
    );

    Ok(())
}

#[tokio::test]
async fn unbackfilled_commit_limit_returns_too_many_requests() -> Result<(), String> {
    let harness = make_harness();
    let table_id = Uuid::now_v7();
    let coordinator_path = format!("delta/coordinator/{table_id}.json");
    let state = arco_delta::DeltaCoordinatorState {
        latest_version: 999,
        inflight: None,
    };
    harness
        .storage
        .put_raw(
            &coordinator_path,
            Bytes::from(
                serde_json::to_vec(&state)
                    .map_err(|err| format!("serialize seeded coordinator state: {err}"))?,
            ),
            WritePrecondition::DoesNotExist,
        )
        .await
        .map_err(|err| format!("seed coordinator state: {err}"))?;

    let response = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path",
            "commit_info": commit_info(1000)
        }),
        Some(&Uuid::now_v7().to_string()),
        Some("req-uc-cap"),
    )
    .await?;

    assert_eq!(response.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        response
            .body
            .pointer("/error/error_code")
            .and_then(Value::as_str),
        Some("TOO_MANY_REQUESTS")
    );

    let staged_paths = harness
        .storage
        .list(&format!("delta/staging/{table_id}/"))
        .await
        .map_err(|err| format!("list staged payloads: {err}"))?;
    assert!(
        staged_paths.is_empty(),
        "rejected commit should not leave staged payloads"
    );

    Ok(())
}

#[tokio::test]
async fn successful_commit_cleans_staged_payload() -> Result<(), String> {
    let harness = make_harness();
    let table_id = Uuid::now_v7();
    let idempotency_key = Uuid::now_v7().to_string();

    let response = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path",
            "commit_info": commit_info(0)
        }),
        Some(&idempotency_key),
        Some("req-uc-cleanup"),
    )
    .await?;
    assert_eq!(response.status, StatusCode::OK);

    let staged_paths = harness
        .storage
        .list(&format!("delta/staging/{table_id}/"))
        .await
        .map_err(|err| format!("list staged payloads: {err}"))?;
    assert!(
        staged_paths.is_empty(),
        "successful commit should remove staged payload"
    );

    Ok(())
}

#[tokio::test]
async fn expired_inflight_is_recovered_and_unrelated_key_is_conflict() -> Result<(), String> {
    let harness = make_harness();
    let table_id = Uuid::now_v7();
    let inflight_key = Uuid::now_v7().to_string();
    let staged_path = format!("delta/staging/{table_id}/{inflight_key}.json");
    let staged_payload = json!({
        "commitInfo": commit_info(0)
    });

    let staged_write = harness
        .storage
        .put_raw(
            &staged_path,
            Bytes::from(staged_payload.to_string()),
            WritePrecondition::DoesNotExist,
        )
        .await
        .map_err(|err| format!("seed staged payload: {err}"))?;

    let staged_version = match staged_write {
        arco_core::storage::WriteResult::Success { version } => version,
        arco_core::storage::WriteResult::PreconditionFailed { .. } => {
            return Err("unexpected staged precondition failure".to_string());
        }
    };

    let now_ms = chrono::Utc::now().timestamp_millis();
    let state = arco_delta::DeltaCoordinatorState {
        latest_version: -1,
        inflight: Some(arco_delta::InflightCommit {
            commit_id: inflight_key.clone(),
            read_version: Some(-1),
            version: 0,
            staged_path: staged_path.clone(),
            staged_version,
            started_at_ms: now_ms - 60_000,
            expires_at_ms: now_ms - 1_000,
        }),
    };

    harness
        .storage
        .put_raw(
            &format!("delta/coordinator/{table_id}.json"),
            Bytes::from(
                serde_json::to_vec(&state)
                    .map_err(|err| format!("serialize seeded coordinator state: {err}"))?,
            ),
            WritePrecondition::DoesNotExist,
        )
        .await
        .map_err(|err| format!("seed coordinator state: {err}"))?;

    let conflict = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path",
            "commit_info": commit_info(0)
        }),
        Some(&Uuid::now_v7().to_string()),
        Some("req-uc-inflight"),
    )
    .await?;

    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(
        conflict
            .body
            .pointer("/error/error_code")
            .and_then(Value::as_str),
        Some("CONFLICT")
    );
    assert_eq!(conflict.request_id.as_deref(), Some("req-uc-inflight"));

    let listed = uc_request(
        &harness.router,
        Method::GET,
        "/delta/preview/commits",
        json!({
            "table_id": table_id,
            "table_uri": "gs://bucket/path",
            "start_version": 0
        }),
        None,
        None,
    )
    .await?;

    assert_eq!(listed.status, StatusCode::OK);
    assert_eq!(
        listed
            .body
            .get("latest_table_version")
            .and_then(Value::as_i64),
        Some(0)
    );
    assert_eq!(
        listed
            .body
            .get("commits")
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );

    Ok(())
}
