//! Delta preview commit semantics tests for UC facade coordinator mapping.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use arco_catalog::authz::privileges::Privilege;
use arco_catalog::{
    CatalogWriter, ColumnDefinition, RegisterTableInSchemaRequest, Tier1Compactor, WriteOptions,
};
use arco_core::ScopedStorage;
use arco_core::storage::{
    ListPage, MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use bytes::Bytes;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

#[path = "support/delta_preview.rs"]
mod delta_preview;

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

async fn make_harness(location: &str) -> Result<(Harness, Uuid), String> {
    make_harness_with_backend(location, Arc::new(MemoryBackend::new())).await
}

async fn make_harness_with_backend(
    location: &str,
    backend: Arc<dyn StorageBackend>,
) -> Result<(Harness, Uuid), String> {
    let storage = ScopedStorage::new(backend, "tenant1", "workspace1").expect("scoped storage");
    let table_id = seed_managed_delta_table(&storage, location).await?;
    let router = delta_preview::router_with_table_privileges(
        &storage,
        &[&table_id.to_string()],
        &[Privilege::Select, Privilege::Modify],
    );
    Ok((Harness { router, storage }, table_id))
}

fn catalog_writer(storage: &ScopedStorage) -> CatalogWriter {
    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    CatalogWriter::new(storage.clone()).with_sync_compactor(compactor)
}

async fn seed_managed_delta_table(storage: &ScopedStorage, location: &str) -> Result<Uuid, String> {
    let writer = catalog_writer(storage);
    writer
        .initialize()
        .await
        .map_err(|err| format!("initialize catalog: {err}"))?;
    writer
        .create_catalog("prod", Some("production"), WriteOptions::default())
        .await
        .map_err(|err| format!("create catalog: {err}"))?;
    writer
        .create_schema(
            "prod",
            "analytics",
            Some("analytics"),
            WriteOptions::default(),
        )
        .await
        .map_err(|err| format!("create schema: {err}"))?;
    let table = writer
        .register_table_in_schema(
            "prod",
            "analytics",
            RegisterTableInSchemaRequest {
                name: "orders".to_string(),
                description: None,
                location: Some(location.to_string()),
                format: Some("delta".to_string()),
                table_type: Some("MANAGED".to_string()),
                properties: None,
                columns: vec![ColumnDefinition {
                    name: "id".to_string(),
                    data_type: "STRING".to_string(),
                    is_nullable: false,
                    ordinal: 0,
                    description: None,
                }],
            },
            WriteOptions::default(),
        )
        .await
        .map_err(|err| format!("seed table: {err}"))?;

    Uuid::parse_str(&table.id).map_err(|err| format!("parse table id: {err}"))
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
        .extension(delta_preview::trusted_context(
            "tenant1",
            "workspace1",
            idempotency_key,
            request_id,
        ))
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

#[derive(Debug, Default)]
struct BoundedLogBackend(MemoryBackend);

#[async_trait::async_trait]
impl StorageBackend for BoundedLogBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.0.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.0.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.0.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.0.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        if prefix.contains("/_delta_log/") {
            return Err(arco_core::Error::storage(
                "unbounded Delta history enumeration",
            ));
        }
        self.0.list(prefix).await
    }

    async fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> arco_core::Result<ListPage> {
        self.0.list_page(prefix, start_after, limit).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.0.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.0.signed_url(path, expiry).await
    }
}

#[tokio::test]
async fn normal_delta_preview_operations_need_no_unbounded_log_listing() -> Result<(), String> {
    let location = "gs://bucket/path";
    let (harness, id) =
        make_harness_with_backend(location, Arc::new(BoundedLogBackend::default())).await?;
    let commit = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({"table_id": id, "table_uri": location, "commit_info": commit_info(0)}),
        Some(&Uuid::now_v7().to_string()),
        None,
    )
    .await?;
    assert_eq!(commit.status, StatusCode::OK, "{}", commit.body);

    let history = uc_request(
        &harness.router,
        Method::GET,
        "/delta/preview/commits",
        json!({"table_id": id, "table_uri": location}),
        None,
        None,
    )
    .await?;
    assert_eq!(history.status, StatusCode::OK, "{}", history.body);
    assert_eq!(
        history
            .body
            .get("latest_table_version")
            .and_then(Value::as_i64),
        Some(0)
    );
    assert_eq!(
        history
            .body
            .pointer("/commits/0/version")
            .and_then(Value::as_i64),
        Some(0)
    );

    let backfill = uc_request(
        &harness.router,
        Method::POST,
        "/delta/preview/commits",
        json!({"table_id": id, "table_uri": location, "latest_backfilled_version": 0}),
        None,
        None,
    )
    .await?;
    assert_eq!(backfill.status, StatusCode::OK, "{}", backfill.body);
    Ok(())
}

#[tokio::test]
async fn delta_preview_history_bounds_the_range_to_finalized_versions() -> Result<(), String> {
    let location = "gs://bucket/path";
    let (harness, id) = make_harness(location).await?;
    let finalized = 10_000;
    harness
        .storage
        .put_raw(
            &format!("delta/coordinator/{id}.json"),
            Bytes::from(
                json!({"table_root": "path", "latest_version": finalized, "inflight": null})
                    .to_string(),
            ),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed finalized coordinator state");
    for version in [finalized - 1, finalized, finalized + 1] {
        harness
            .storage
            .put_raw(
                &format!("path/_delta_log/{version:020}.json"),
                Bytes::from(json!({"commitInfo": commit_info(version)}).to_string()),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("seed Delta log");
    }
    let wide = uc_request(
        &harness.router,
        Method::GET,
        "/delta/preview/commits",
        json!({"table_id": id, "table_uri": location}),
        None,
        None,
    )
    .await?;
    assert_eq!(wide.status, StatusCode::BAD_REQUEST, "{}", wide.body);
    let bounded = uc_request(
        &harness.router,
        Method::GET,
        "/delta/preview/commits",
        json!({
            "table_id": id, "table_uri": location,
            "start_version": finalized - 1, "end_version": finalized + 1,
        }),
        None,
        None,
    )
    .await?;
    assert_eq!(bounded.status, StatusCode::OK, "{}", bounded.body);
    assert_eq!(
        bounded
            .body
            .get("latest_table_version")
            .and_then(Value::as_i64),
        Some(finalized)
    );
    let commits = bounded
        .body
        .get("commits")
        .and_then(Value::as_array)
        .expect("Delta history");
    let versions = commits
        .iter()
        .map(|commit| commit.get("version").and_then(Value::as_i64))
        .collect::<Vec<_>>();
    assert_eq!(versions, [Some(finalized - 1), Some(finalized)]);

    harness
        .storage
        .put_raw(
            &format!("path/_delta_log/{finalized:020}.json"),
            Bytes::from(vec![b' '; 8 * 1024 * 1024 + 1]),
            WritePrecondition::None,
        )
        .await
        .expect("seed oversized log");
    let oversized = uc_request(
        &harness.router,
        Method::GET,
        "/delta/preview/commits",
        json!({"table_id": id, "table_uri": location, "start_version": finalized}),
        None,
        None,
    )
    .await?;
    assert_eq!(
        oversized.status,
        StatusCode::BAD_REQUEST,
        "{}",
        oversized.body
    );
    Ok(())
}

#[tokio::test]
async fn delta_preview_log_only_history_requires_explicit_migration() -> Result<(), String> {
    let location = "gs://bucket/path";
    let (harness, id) =
        make_harness_with_backend(location, Arc::new(BoundedLogBackend::default())).await?;
    harness
        .storage
        .put_raw(
            "path/_delta_log/00000000000000000000.json",
            Bytes::from(json!({"commitInfo": commit_info(0)}).to_string()),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed an unmanaged log");
    let history = uc_request(
        &harness.router,
        Method::GET,
        "/delta/preview/commits",
        json!({"table_id": id, "table_uri": location, "end_version": 0}),
        None,
        None,
    )
    .await?;
    assert_eq!(history.status, StatusCode::CONFLICT, "{}", history.body);
    assert_eq!(
        history
            .body
            .pointer("/error/message")
            .and_then(Value::as_str),
        Some("delta_preview_log_migration_required")
    );
    Ok(())
}

#[tokio::test]
async fn idempotency_replay_is_deduplicated_in_get_commits() -> Result<(), String> {
    let (harness, table_id) = make_harness("gs://bucket/path").await?;
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
    let (harness, table_id) = make_harness("gs://bucket/path").await?;

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
    let (harness, table_id) = make_harness("gs://bucket/path").await?;

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
    let (harness, table_id) = make_harness("gs://bucket/path-a").await?;

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
    let (harness, table_id) = make_harness("gs://bucket/path").await?;
    let coordinator_path = format!("delta/coordinator/{table_id}.json");
    let state = json!({"table_root": "path", "latest_version": 999, "inflight": null});
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
    let (harness, table_id) = make_harness("gs://bucket/path").await?;
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
    let (harness, table_id) = make_harness("gs://bucket/path").await?;
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
        WriteResult::Success { version } => version,
        WriteResult::PreconditionFailed { .. } => {
            return Err("unexpected staged precondition failure".to_string());
        }
    };

    let now_ms = chrono::Utc::now().timestamp_millis();
    let state = json!({
        "table_root": "path",
        "latest_version": -1,
        "inflight": arco_delta::InflightCommit {
            commit_id: inflight_key.clone(),
            read_version: Some(-1),
            version: 0,
            staged_path: staged_path.clone(),
            staged_version,
            started_at_ms: now_ms - 60_000,
            expires_at_ms: now_ms - 1_000,
        },
    });

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
