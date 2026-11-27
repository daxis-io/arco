//! Production authorization coverage for the bounded Unity Catalog table-read extension.
//!
//! The fixture deliberately uses the deployed API composition, authoritative
//! metastore replay, catalog writer, published storage-governance projection,
//! and a scoped storage object. It must not be replaced by an injected
//! permission view: this route is the Arco-owned data authorization boundary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arco_api::config::{Config, JwtConfig, Posture};
use arco_api::server::ServerBuilder;
use arco_catalog::metastore::events::{
    CatalogObjectRecord, ExternalLocationRecord, GrantRecord, LifecycleState, MetastoreEvent,
    MetastoreMutation, PrincipalKind, PrincipalRecord, StorageCredentialRecord,
    WorkspaceBindingRecord,
};
use arco_catalog::metastore::ledger::MetastoreLedger;
use arco_catalog::metastore::projections::ProjectionRegistry;
use arco_catalog::metastore::publish::publish_current_metastore_projection;
use arco_catalog::{
    CatalogWriter, ColumnDefinition, RegisterTableInSchemaRequest, RegisterTableRequest,
    Tier1Compactor, WriteOptions,
};
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{ControlPlaneScope, DeltaPaths, ScopedStorage};
use arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use uuid::Uuid;

#[path = "support/event_streams.rs"]
mod event_streams;
#[path = "support/governed_catalog_read.rs"]
mod governed_catalog_read;

#[path = "support/usage.rs"]
mod usage;

#[cfg(feature = "aws")]
#[path = "support/managed_delta_placement.rs"]
mod managed_delta_placement;

const TENANT: &str = "tenant1";
const WORKSPACE: &str = "workspace1";
const READER: &str = "user_reader";
const INTRUDER: &str = "user_intruder";
const STORAGE_ROOT: &str = "s3://table-read-bucket";
const OBJECT_PATH: &str = "fixtures/orders.parquet";
const JWT_ISSUER: &str = "https://issuer.table-read.test";
const JWT_AUDIENCE: &str = "arco-table-read";

const TEST_RSA_PRIVATE_KEY_PEM: &str = r"-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL
UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V
rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H
oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI
Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/
by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd
WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ
Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j
E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn
LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS
bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE
8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl
xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY
2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1
9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0
7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ
mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7
MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4
t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam
QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA
2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ
4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn
PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH
UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8
oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==
-----END RSA PRIVATE KEY-----";

const TEST_RSA_PUBLIC_KEY_PEM: &str = r"-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4
l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2VrUyW
yj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG
/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4l
QzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/by2h
3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQAB
-----END RSA PUBLIC KEY-----";

/// An authenticated principal with a real current TABLE SELECT grant and an
/// active, workspace-bound governed location receives only the cataloged
/// bounded Parquet bytes and redacted binding evidence.
///
/// Before the extension is mounted this intentionally fails with the router's
/// missing-route 404. It is the first red test for the owner path.
#[tokio::test]
async fn production_owner_path_returns_bounded_authorized_parquet() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("production UC response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/vnd.apache.parquet")
    );
    assert_eq!(
        response
            .headers()
            .get("X-Arco-Table-Read-Tenant")
            .and_then(|value| value.to_str().ok()),
        Some(TENANT)
    );
    assert_eq!(
        response
            .headers()
            .get("X-Arco-Table-Read-Workspace")
            .and_then(|value| value.to_str().ok()),
        Some(WORKSPACE)
    );
    assert_eq!(
        response
            .headers()
            .get("X-Arco-Table-Read-Table-Id")
            .and_then(|value| value.to_str().ok()),
        Some(fixture.table_id.as_str())
    );
    assert_eq!(
        response
            .headers()
            .get("X-Arco-Table-Read-Enforcement")
            .and_then(|value| value.to_str().ok()),
        Some("table_select,storage_governance;row_filter=unsupported;column_mask=unsupported")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    assert_eq!(
        response
            .headers()
            .get("X-Arco-Table-Read-Version")
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );
    let expected_byte_size = fixture.parquet.len().to_string();
    assert_eq!(
        response
            .headers()
            .get("X-Arco-Table-Read-Byte-Size")
            .and_then(|value| value.to_str().ok()),
        Some(expected_byte_size.as_str())
    );
    for name in [
        "X-Arco-Table-Read-Principal-SHA256",
        "X-Arco-Table-Read-Table-Fingerprint",
        "X-Arco-Table-Read-Object-Version-SHA256",
        "X-Arco-Table-Read-Content-SHA256",
        "X-Arco-Table-Read-Physical-Schema-SHA256",
        "X-Arco-Table-Read-Snapshot-Id",
        "X-Arco-Table-Read-Permission-Watermark",
        "X-Arco-Table-Read-Authorization-Audit-Id",
    ] {
        assert!(
            response.headers().get(name).is_some(),
            "missing required table-read descriptor header: {name}"
        );
    }
    assert!(
        response
            .headers()
            .get("X-Arco-Table-Read-Object-Path")
            .is_none(),
        "the response must not disclose the catalog object path"
    );

    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("bounded Parquet response body");
    assert_eq!(body, fixture.parquet);
}

/// The descriptor is a metadata-only view over the same owner-authorized,
/// immutable snapshot as the bounded read. It never turns loading time into a
/// completeness claim.
#[tokio::test]
async fn production_owner_path_returns_pinned_dataset_descriptor() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_descriptor_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("production descriptor response");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("descriptor body");
    let descriptor: Value = serde_json::from_slice(&body).expect("descriptor JSON");
    assert_eq!(descriptor["dataset_id"], fixture.table_id);
    assert_eq!(descriptor["completeness"], "unknown");
    assert_eq!(descriptor["loaded_at"], Value::Null);
    assert_eq!(descriptor["source_data_cutoff"], Value::Null);
    assert_eq!(descriptor["schema"][0]["name"], "revenue");
    assert_eq!(descriptor["input_identity"]["table_id"], fixture.table_id);
    assert!(
        descriptor["input_identity"]["snapshot_id"]
            .as_str()
            .is_some_and(|value| value.len() == 64)
    );
    assert!(
        descriptor["semantic_metadata_identity"]
            .as_str()
            .is_some_and(|value| value.len() == 64)
    );
}

#[tokio::test]
async fn production_owner_path_denies_descriptor_without_current_select() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_descriptor_request(
            &fixture.table_id,
            INTRUDER,
            &serde_json::json!({}),
        ))
        .await
        .expect("production descriptor denial response");
    assert_uc_error(response, StatusCode::FORBIDDEN, "FORBIDDEN", None).await;
}

#[tokio::test]
async fn production_owner_path_descriptor_reports_owner_semantics() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend.clone(), TENANT, WORKSPACE).unwrap();
    let writer = table_read_catalog(&storage).await;
    let table = writer
        .register_table_in_schema(
            "default",
            "analytics",
            RegisterTableInSchemaRequest {
                name: "orders".to_string(),
                description: Some("Owner-defined order revenue.".to_string()),
                location: Some(format!(
                    "{STORAGE_ROOT}/tenant={TENANT}/workspace={WORKSPACE}/{OBJECT_PATH}"
                )),
                format: Some("parquet".to_string()),
                table_type: Some("MANAGED".to_string()),
                properties: Some(BTreeMap::from([
                    (
                        "arco.dataset.grain".to_string(),
                        "one row per order".to_string(),
                    ),
                    (
                        "arco.dataset.units".to_string(),
                        r#"{"revenue":"USD cents"}"#.to_string(),
                    ),
                    (
                        "arco.dataset.source_data_cutoff".to_string(),
                        "2026-11-03T00:00:00Z".to_string(),
                    ),
                    (
                        "arco.dataset.loaded_at".to_string(),
                        "not-a-date-time".to_string(),
                    ),
                ])),
                columns: vec![revenue_column()],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register cataloged Parquet with owner semantics");
    storage
        .put_raw(
            OBJECT_PATH,
            single_column_parquet(),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    seed_table_read_authority(&storage, STORAGE_ROOT, &table.id, &table.name, READER, true).await;
    let router = production_router(backend);
    let response = router
        .oneshot(table_descriptor_request(
            &table.id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("owner descriptor response");
    let descriptor: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("owner descriptor body"),
    )
    .expect("owner descriptor JSON");
    assert_eq!(descriptor["meaning"], "Owner-defined order revenue.");
    assert_eq!(descriptor["grain"], "one row per order");
    assert_eq!(descriptor["units"]["revenue"], "USD cents");
    assert_eq!(descriptor["source_data_cutoff"], "2026-11-03T00:00:00Z");
    assert_eq!(descriptor["loaded_at"], Value::Null);
    assert!(
        descriptor["unavailable_facts"]
            .as_array()
            .is_some_and(|facts| facts.iter().any(|fact| fact == "loaded_at"))
    );
    assert_eq!(descriptor["completeness"], "unknown");
}

#[tokio::test]
async fn production_table_reads_are_default_off() {
    let router = production_router_with_gate(Arc::new(MemoryBackend::new()), false);

    let response = router
        .oneshot(table_read_request(
            "unregistered-table",
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("UC response");

    assert_uc_error(response, StatusCode::NOT_FOUND, "NOT_FOUND", None).await;
}

#[tokio::test]
async fn production_owner_path_denies_principal_without_select() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_read_request(
            &fixture.table_id,
            INTRUDER,
            &serde_json::json!({}),
        ))
        .await
        .expect("UC response");

    assert_uc_error(response, StatusCode::FORBIDDEN, "FORBIDDEN", None).await;
}

#[tokio::test]
async fn production_owner_path_denies_revoked_select_after_permission_source_refresh() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_router(backend);

    let allowed = router
        .clone()
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("initial UC response");
    assert_eq!(allowed.status(), StatusCode::OK);

    revoke_fixture_select(&fixture, READER).await;

    let denied = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("revoked UC response");
    assert_uc_error(denied, StatusCode::FORBIDDEN, "FORBIDDEN", None).await;
}

#[tokio::test]
async fn production_owner_path_rejects_cross_scope_table_selection() {
    let backend = Arc::new(MemoryBackend::new());
    let _first_scope = seed_table_read_fixture(Arc::clone(&backend)).await;
    let second_scope = seed_table_read_fixture_in_scope(
        Arc::clone(&backend),
        "tenant2",
        "workspace2",
        None,
        "parquet",
        single_column_parquet(),
        true,
    )
    .await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_read_request_in_scope(
            &second_scope.table_id,
            TENANT,
            WORKSPACE,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("cross-scope UC response");

    assert_uc_error(response, StatusCode::FORBIDDEN, "FORBIDDEN", None).await;
}

#[tokio::test]
async fn production_owner_path_rejects_cross_scope_descriptor_selection() {
    let backend = Arc::new(MemoryBackend::new());
    let _first_scope = seed_table_read_fixture(Arc::clone(&backend)).await;
    let second_scope = seed_table_read_fixture_in_scope(
        Arc::clone(&backend),
        "tenant2",
        "workspace2",
        None,
        "parquet",
        single_column_parquet(),
        true,
    )
    .await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_descriptor_request_in_scope(
            &second_scope.table_id,
            TENANT,
            WORKSPACE,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("cross-scope descriptor response");
    assert_uc_error(response, StatusCode::FORBIDDEN, "FORBIDDEN", None).await;
}

#[tokio::test]
async fn production_owner_path_rejects_catalog_location_outside_configured_scope() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture_in_scope(
        Arc::clone(&backend),
        TENANT,
        WORKSPACE,
        Some("s3://other-bucket/tenant=tenant1/workspace=workspace1/orders.parquet".to_string()),
        "parquet",
        single_column_parquet(),
        true,
    )
    .await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("UC response");

    assert_uc_error(
        response,
        StatusCode::BAD_REQUEST,
        "BAD_REQUEST",
        Some("table_read_location_not_mapped"),
    )
    .await;
}

#[tokio::test]
async fn production_owner_path_fails_closed_without_published_storage_governance() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture_in_scope(
        Arc::clone(&backend),
        TENANT,
        WORKSPACE,
        None,
        "parquet",
        single_column_parquet(),
        false,
    )
    .await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("UC response");

    assert_uc_error(
        response,
        StatusCode::SERVICE_UNAVAILABLE,
        "SERVICE_UNAVAILABLE",
        None,
    )
    .await;
}

#[tokio::test]
async fn production_owner_path_fails_closed_when_governance_is_stale_after_ledger_advance() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_router(backend);

    let allowed = router
        .clone()
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("initial UC response");
    assert_eq!(allowed.status(), StatusCode::OK);

    append_unrelated_principal_with_publication(&fixture, false).await;

    let response = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("stale-governance UC response");

    assert_uc_error(
        response,
        StatusCode::SERVICE_UNAVAILABLE,
        "SERVICE_UNAVAILABLE",
        None,
    )
    .await;
}

#[tokio::test]
async fn production_owner_path_rejects_unsupported_format() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture_in_scope(
        Arc::clone(&backend),
        TENANT,
        WORKSPACE,
        None,
        "delta",
        single_column_parquet(),
        true,
    )
    .await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("UC response");

    assert_uc_error(
        response,
        StatusCode::BAD_REQUEST,
        "BAD_REQUEST",
        Some("table_read_unsupported_format"),
    )
    .await;
}

#[tokio::test]
async fn production_managed_delta_allocation_requires_approved_root() {
    let router = production_rs256_router(Arc::new(MemoryBackend::new()), STORAGE_ROOT);
    let bearer = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let (status, _) = post_json(
        &router,
        "/api/v1/managed-delta/ensure",
        &bearer,
        json!({
            "operation_id": Uuid::now_v7(),
            "owner": READER,
            "provisioning_fingerprint": "allocation-test-v1"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn production_native_delta_stage_requires_modify() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (fixture, router, _) = managed_delta_fixture(backend, STORAGE_ROOT, WORKSPACE).await;
    let token = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    set_fixture_modify(&fixture, LifecycleState::Deleted).await;
    let before = fixture_object_versions(&fixture).await;
    let (status, body) = post_json(
        &router,
        &format!("/api/v1/delta/tables/{}/commits/stage", fixture.table_id),
        &token,
        json!({"payload": "{\"commitInfo\":{}}\n"}),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(fixture_object_versions(&fixture).await, before);
}

#[tokio::test]
async fn production_native_delta_commit_rechecks_modify_after_stage() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (fixture, router, paths) = managed_delta_fixture(backend, STORAGE_ROOT, WORKSPACE).await;
    let token = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let log = delta_orders_commit(&fixture, &paths, 0, &[1_000], None).await;
    let path = format!("/api/v1/delta/tables/{}/commits", fixture.table_id);
    let (status, staged) = post_json(
        &router,
        &format!("{path}/stage"),
        &token,
        json!({"payload": log}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{staged}");

    set_fixture_modify(&fixture, LifecycleState::Deleted).await;
    let before = fixture_object_versions(&fixture).await;
    let commit = json!({
        "read_version": -1,
        "staged_path": staged.get("staged_path").expect("staged path"),
        "staged_version": staged.get("staged_version").expect("staged version"),
    });
    let (status, body) = post_json(&router, &path, &token, commit.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(fixture_object_versions(&fixture).await, before);

    set_fixture_modify(&fixture, LifecycleState::Active).await;
    let (status, body) = post_json(&router, &path, &token, commit).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body.get("version").and_then(Value::as_i64), Some(0));
}

#[tokio::test]
async fn production_owner_path_describes_a_committed_managed_delta_version() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    exercise_delta_owner_reads(backend, STORAGE_ROOT, None).await;
}

#[tokio::test]
async fn production_owner_path_rejects_replaced_committed_delta_objects() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let (fixture, router, paths) = managed_delta_fixture(backend, STORAGE_ROOT, WORKSPACE).await;
    let token = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let payload = delta_orders_commit(&fixture, &paths, 0, &[1_000, -200], None).await;
    commit_delta(&router, &fixture.table_id, &token, 0, &payload).await;

    let descriptor_path = extension_path("table-descriptors", &fixture.table_id);
    for path in [
        paths.delta_log_json(0).unwrap(),
        format!("{}/orders-0.parquet", paths.table_root()),
    ] {
        let original = fixture.storage.get_raw(&path).await.unwrap();
        let mut replacement = original.to_vec();
        replacement[0] ^= 1;
        fixture
            .storage
            .put_raw(&path, replacement.into(), WritePrecondition::None)
            .await
            .unwrap();
        let (status, error) = post_json(
            &router,
            &descriptor_path,
            &token,
            json!({"delta_version": 0}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{error}");
        assert_eq!(
            error.pointer("/error/message").and_then(Value::as_str),
            Some("delta_read_object_identity_mismatch")
        );
        fixture
            .storage
            .put_raw(&path, original, WritePrecondition::None)
            .await
            .unwrap();
    }
}

#[cfg(feature = "aws")]
#[tokio::test]
#[ignore = "requires fresh MinIO and a pinned delta-rs Python environment"]
async fn production_owner_path_minio_native_delta() {
    let bucket = std::env::var("ARCO_DELTA_PROOF_BUCKET").expect("fresh MinIO s3:// bucket");
    let evidence = std::env::var("ARCO_DELTA_PROOF_EVIDENCE").expect("fresh proof directory");
    let backend = arco_storage::from_bucket(&bucket).expect("real S3 adapter");
    exercise_delta_owner_reads(backend, &bucket, Some(Path::new(&evidence))).await;
}

async fn exercise_delta_owner_reads(
    backend: Arc<dyn StorageBackend>,
    storage_root: &str,
    evidence: Option<&Path>,
) {
    let (fixture, router, paths) =
        managed_delta_fixture(Arc::clone(&backend), storage_root, WORKSPACE).await;
    let txn = delta_orders_commit(&fixture, &paths, 0, &[1_000, -200], None).await;
    let token = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let descriptor_path = extension_path("table-descriptors", &fixture.table_id);
    commit_delta(&router, &fixture.table_id, &token, 0, &txn).await;

    let (status, first_descriptor) = post_json(
        &router,
        &descriptor_path,
        &token,
        json!({"delta_version": 0}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first_descriptor}");
    let first_identity = first_descriptor.get("input_identity").unwrap();
    assert_eq!(
        first_identity.get("table_id").and_then(Value::as_str),
        Some(fixture.table_id.as_str())
    );
    assert_eq!(
        first_identity.get("delta_version").and_then(Value::as_i64),
        Some(0)
    );
    assert_eq!(
        first_descriptor
            .pointer("/schema/0/name")
            .and_then(Value::as_str),
        Some("day")
    );
    assert_eq!(
        first_descriptor
            .pointer("/units/amount_cents")
            .and_then(Value::as_str),
        Some("USD cents")
    );
    let first_pin = first_identity
        .get("snapshot_id")
        .and_then(Value::as_str)
        .unwrap();
    let txn = delta_orders_commit(&fixture, &paths, 1, &[300], None).await;
    commit_delta(&router, &fixture.table_id, &token, 1, &txn).await;

    for expected in [(0, 800, 2), (1, 1_100, 3)] {
        assert_delta_snapshot(
            &router,
            &fixture.table_id,
            &token,
            first_identity,
            expected,
            evidence,
        )
        .await;
    }
    assert_delta_read_denials(
        backend,
        storage_root,
        &router,
        &fixture,
        &paths,
        first_pin,
        evidence,
    )
    .await;
}

async fn assert_delta_snapshot(
    router: &Router,
    table_id: &str,
    token: &str,
    first_identity: &Value,
    expected: (i64, i64, i64),
    evidence: Option<&Path>,
) {
    let (version, amount, count) = expected;
    let first_pin = first_identity
        .get("snapshot_id")
        .and_then(Value::as_str)
        .unwrap();
    let descriptor_path = extension_path("table-descriptors", table_id);
    let read_path = extension_path("table-reads", table_id);
    let describe = if version == 0 {
        json!({"delta_version": version, "expected_snapshot_id": first_pin})
    } else {
        json!({"delta_version": version})
    };
    let (status, descriptor) = post_json(router, &descriptor_path, token, describe.clone()).await;
    assert_eq!(status, StatusCode::OK, "{descriptor}");
    let identity = descriptor.get("input_identity").unwrap();
    let pin = identity.get("snapshot_id").and_then(Value::as_str).unwrap();
    assert_eq!(
        identity.get("table_id").and_then(Value::as_str),
        Some(table_id)
    );
    assert_eq!(
        identity.get("workspace").and_then(Value::as_str),
        Some(WORKSPACE)
    );
    assert_eq!(
        identity.get("delta_version").and_then(Value::as_i64),
        Some(version)
    );
    if version == 0 {
        assert_eq!(identity, first_identity);
    } else {
        assert_ne!(pin, first_pin);
    }
    let read = json!({"delta_version": version, "expected_snapshot_id": pin});
    let (status, bundle) = post_json(router, &read_path, token, read.clone()).await;
    assert_eq!(status, StatusCode::OK, "{bundle}");
    assert_eq!(bundle.get("input_identity"), Some(identity));
    assert_eq!(
        bundle
            .get("objects")
            .and_then(Value::as_array)
            .unwrap()
            .len(),
        2 * (usize::try_from(version).unwrap() + 1)
    );
    if let Some(dir) = evidence {
        write_proof_json(
            dir,
            &format!("v{version}-requests.json"),
            &json!({
                "describe": {"method": "POST", "path": descriptor_path, "body": describe},
                "read": {"method": "POST", "path": read_path, "body": read},
            }),
        );
        let observed = run_native_delta_reader(dir, version, &descriptor, &bundle);
        assert_eq!(
            observed.get("table_id").and_then(Value::as_str),
            Some(table_id)
        );
        assert_eq!(
            observed.get("snapshot_id").and_then(Value::as_str),
            Some(pin)
        );
        assert_eq!(
            observed.get("delta_version").and_then(Value::as_i64),
            Some(version)
        );
        assert_eq!(
            observed.get("total_cents").and_then(Value::as_i64),
            Some(amount)
        );
        assert_eq!(
            observed.get("order_count").and_then(Value::as_i64),
            Some(count)
        );
    }
}

async fn assert_delta_read_denials(
    backend: Arc<dyn StorageBackend>,
    storage_root: &str,
    router: &Router,
    fixture: &TableReadFixture,
    paths: &DeltaPaths,
    first_pin: &str,
    evidence: Option<&Path>,
) {
    let pinned_v0 = json!({"delta_version": 0, "expected_snapshot_id": first_pin});
    let token = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let descriptor_path = extension_path("table-descriptors", &fixture.table_id);
    let read_path = extension_path("table-reads", &fixture.table_id);
    let [
        (unsupported_id, unsupported_token),
        (outside_id, outside_token),
    ] = commit_delta_denial_tables(backend, storage_root, router).await;
    let uncommitted_path = paths.delta_log_json(2).unwrap();
    fixture
        .storage
        .put_raw(
            &uncommitted_path,
            Bytes::from_static(b"{\"commitInfo\":{}}\n"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .unwrap();
    let mut denials = BTreeMap::new();
    for (name, path, bearer, request, expected_status, reason) in [
        (
            "cross_workspace",
            read_path.clone(),
            unsupported_token.as_str(),
            pinned_v0.clone(),
            StatusCode::FORBIDDEN,
            None,
        ),
        (
            "bad_pin",
            read_path.clone(),
            token.as_str(),
            json!({"delta_version": 0, "expected_snapshot_id": "0".repeat(64)}),
            StatusCode::CONFLICT,
            Some("table_read_snapshot_mismatch"),
        ),
        (
            "pin_for_wrong_version",
            read_path.clone(),
            token.as_str(),
            json!({"delta_version": 1, "expected_snapshot_id": first_pin}),
            StatusCode::CONFLICT,
            Some("table_read_snapshot_mismatch"),
        ),
        (
            "missing_pin",
            read_path.clone(),
            token.as_str(),
            json!({"delta_version": 0}),
            StatusCode::BAD_REQUEST,
            Some("delta_read_pin_required"),
        ),
        (
            "uncommitted_object_present",
            descriptor_path.clone(),
            token.as_str(),
            json!({"delta_version": 2}),
            StatusCode::NOT_FOUND,
            Some("delta_read_version_not_committed"),
        ),
        (
            "unsupported_feature",
            extension_path("table-descriptors", &unsupported_id),
            unsupported_token.as_str(),
            json!({"delta_version": 0}),
            StatusCode::BAD_REQUEST,
            Some("delta_read_unsupported_log"),
        ),
        (
            "out_of_scope_object",
            extension_path("table-descriptors", &outside_id),
            outside_token.as_str(),
            json!({"delta_version": 0}),
            StatusCode::BAD_REQUEST,
            Some("delta_read_out_of_scope_object"),
        ),
    ] {
        let (status, error) = post_json(router, &path, bearer, request.clone()).await;
        assert_eq!(status, expected_status, "{name}: {error}");
        if let Some(reason) = reason {
            assert_eq!(
                error.pointer("/error/message").and_then(Value::as_str),
                Some(reason),
                "{name}: {error}"
            );
        }
        denials.insert(
            name,
            json!({
                "path": path, "request": request, "status": status.as_u16(), "response": error,
            }),
        );
    }
    denials.extend(assert_delta_revocations(router, fixture, &read_path, &token, &pinned_v0).await);
    if let Some(dir) = evidence {
        write_proof_json(dir, "denials.json", &json!(denials));
    }
}

async fn assert_delta_revocations(
    router: &Router,
    fixture: &TableReadFixture,
    read_path: &str,
    token: &str,
    pinned_v0: &Value,
) -> [(&'static str, Value); 2] {
    set_fixture_storage_binding(fixture, LifecycleState::Deleted).await;
    let (status, error) = post_json(router, read_path, token, pinned_v0.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{error}");
    let storage_denial = (
        "governance_binding_revoked_after_descriptor",
        json!({
            "path": read_path, "request": pinned_v0, "status": status.as_u16(), "response": error,
        }),
    );
    set_fixture_storage_binding(fixture, LifecycleState::Active).await;
    let (status, bundle) = post_json(router, read_path, token, pinned_v0.clone()).await;
    assert_eq!(status, StatusCode::OK, "{bundle}");

    revoke_fixture_select(fixture, READER).await;
    let (status, error) = post_json(router, read_path, token, pinned_v0.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{error}");
    let select_denial = (
        "select_revoked_after_descriptor",
        json!({
            "path": read_path, "request": pinned_v0, "status": status.as_u16(), "response": error,
        }),
    );
    [storage_denial, select_denial]
}

async fn commit_delta_denial_tables(
    backend: Arc<dyn StorageBackend>,
    storage_root: &str,
    router: &Router,
) -> [(String, String); 2] {
    let (unsupported, _, unsupported_paths) =
        managed_delta_fixture(Arc::clone(&backend), storage_root, "feature_control").await;
    let unsupported_token = table_read_rs256_jwt(TENANT, "feature_control", READER);
    let txn = delta_orders_commit(
        &unsupported,
        &unsupported_paths,
        0,
        &[300],
        Some(json!({
            "minReaderVersion": 3, "minWriterVersion": 7,
            "readerFeatures": ["deletionVectors"], "writerFeatures": ["deletionVectors"],
        })),
    )
    .await;
    commit_delta(router, &unsupported.table_id, &unsupported_token, 0, &txn).await;

    let (outside, _, outside_paths) =
        managed_delta_fixture(backend, storage_root, "path_control").await;
    let outside_token = table_read_rs256_jwt(TENANT, "path_control", READER);
    let txn = delta_orders_commit(&outside, &outside_paths, 0, &[300], None)
        .await
        .replace("orders-0.parquet", "../orders-0.parquet");
    commit_delta(router, &outside.table_id, &outside_token, 0, &txn).await;
    [
        (unsupported.table_id, unsupported_token),
        (outside.table_id, outside_token),
    ]
}

async fn set_fixture_storage_binding(fixture: &TableReadFixture, lifecycle_state: LifecycleState) {
    let scope = ControlPlaneScope::workspace_alias(&fixture.tenant, &fixture.workspace).unwrap();
    let ledger = MetastoreLedger::new(fixture.storage.clone()).unwrap();
    let sequence = next_fixture_sequence(&ledger).await;
    let event = MetastoreEvent::new_scoped(
        &scope,
        format!("table_read_change_binding_{sequence}"),
        sequence,
        MetastoreMutation::WorkspaceBindingUpserted(WorkspaceBindingRecord {
            binding_id: "binding_table_read".to_string(),
            workspace_id: fixture.workspace.clone(),
            object_id: "location_table_read".to_string(),
            object_type: "EXTERNAL_LOCATION".to_string(),
            owner: "owner".to_string(),
            lifecycle_state,
            updated_at_ms: 1_800_000_000_010 + i64::try_from(sequence).unwrap(),
            properties: BTreeMap::new(),
        }),
    );
    ledger.append_event(&event).await.unwrap();
    publish_current_metastore_projection(&fixture.storage, &ProjectionRegistry::default())
        .await
        .unwrap();
}

fn write_proof_json(directory: &Path, name: &str, content: &Value) -> std::path::PathBuf {
    let path = directory.join(name);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    serde_json::to_writer_pretty(file, content).unwrap();
    path
}

fn run_native_delta_reader(dir: &Path, version: i64, descriptor: &Value, bundle: &Value) -> Value {
    let descriptor = write_proof_json(dir, &format!("v{version}-descriptor.json"), descriptor);
    let bundle = write_proof_json(dir, &format!("v{version}-bundle.json"), bundle);
    let python = std::env::var_os("ARCO_DELTA_PROOF_PYTHON").expect("pinned Delta reader Python");
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/read_native_delta_snapshot.py");
    let result = std::process::Command::new(python)
        .env_clear()
        .arg("-B")
        .arg(script)
        .arg(descriptor)
        .arg(bundle)
        .output()
        .expect("execute native Delta reader");
    std::fs::write(
        dir.join(format!("v{version}-reader.stderr")),
        &result.stderr,
    )
    .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let observed = serde_json::from_slice(&result.stdout).expect("native Delta reader result");
    write_proof_json(dir, &format!("v{version}-observed.json"), &observed);
    observed
}

#[tokio::test]
async fn production_owner_path_rejects_unimplemented_enforcement_selector() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({ "enforcement": { "row_filter": "attempted" } }),
        ))
        .await
        .expect("UC response");

    assert_uc_error(
        response,
        StatusCode::BAD_REQUEST,
        "BAD_REQUEST",
        Some("table_read_request_invalid"),
    )
    .await;
}

#[tokio::test]
async fn production_owner_path_rejects_invalid_parquet_before_disclosure() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture_in_scope(
        Arc::clone(&backend),
        TENANT,
        WORKSPACE,
        None,
        "parquet",
        Bytes::from_static(b"not a parquet object"),
        true,
    )
    .await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("UC response");

    assert_uc_error(
        response,
        StatusCode::BAD_REQUEST,
        "BAD_REQUEST",
        Some("table_read_invalid_parquet"),
    )
    .await;
}

#[tokio::test]
async fn production_owner_path_enforces_eight_mib_object_limit() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture_in_scope(
        Arc::clone(&backend),
        TENANT,
        WORKSPACE,
        None,
        "parquet",
        Bytes::from(vec![0; 8 * 1024 * 1024 + 1]),
        true,
    )
    .await;
    let router = production_router(backend);

    let response = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("UC response");

    assert_uc_error(
        response,
        StatusCode::PAYLOAD_TOO_LARGE,
        "PAYLOAD_TOO_LARGE",
        Some("table_read_object_too_large"),
    )
    .await;
}

#[tokio::test]
async fn production_owner_path_rejects_expected_snapshot_after_object_replacement() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_router(backend);

    let initial = router
        .clone()
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("initial UC response");
    assert_eq!(initial.status(), StatusCode::OK);
    let expected_snapshot_id = table_read_header(&initial, "X-Arco-Table-Read-Snapshot-Id");

    let pinned = router
        .clone()
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({ "expected_snapshot_id": expected_snapshot_id.clone() }),
        ))
        .await
        .expect("pinned UC response");
    assert_eq!(pinned.status(), StatusCode::OK);
    assert_eq!(
        table_read_header(&pinned, "X-Arco-Table-Read-Snapshot-Id"),
        expected_snapshot_id
    );

    replace_fixture_object(&fixture, single_column_parquet_with_value(84)).await;

    let response = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({ "expected_snapshot_id": expected_snapshot_id }),
        ))
        .await
        .expect("replaced-object UC response");

    assert_uc_error(
        response,
        StatusCode::CONFLICT,
        "CONFLICT",
        Some("table_read_snapshot_mismatch"),
    )
    .await;
}

#[tokio::test]
async fn production_owner_path_descriptor_pin_rejects_changed_snapshot() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_router(backend);
    let initial = router
        .clone()
        .oneshot(table_descriptor_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("initial descriptor response");
    assert_eq!(initial.status(), StatusCode::OK);
    let descriptor: Value = serde_json::from_slice(
        &to_bytes(initial.into_body(), usize::MAX)
            .await
            .expect("initial descriptor body"),
    )
    .expect("initial descriptor JSON");
    let snapshot = descriptor["input_identity"]["snapshot_id"]
        .as_str()
        .expect("descriptor snapshot")
        .to_string();
    replace_fixture_object(&fixture, single_column_parquet_with_value(84)).await;
    let response = router
        .oneshot(table_descriptor_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({ "expected_snapshot_id": snapshot }),
        ))
        .await
        .expect("changed descriptor response");
    assert_uc_error(
        response,
        StatusCode::CONFLICT,
        "CONFLICT",
        Some("table_read_snapshot_mismatch"),
    )
    .await;
}

#[tokio::test]
async fn production_owner_path_snapshot_excludes_unrelated_authorization_watermark() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_router(backend);

    let initial = router
        .clone()
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("initial UC response");
    assert_eq!(initial.status(), StatusCode::OK);
    let initial_snapshot_id = table_read_header(&initial, "X-Arco-Table-Read-Snapshot-Id");
    let initial_watermark = table_read_header(&initial, "X-Arco-Table-Read-Permission-Watermark");

    append_unrelated_principal(&fixture).await;

    let current = router
        .oneshot(table_read_request(
            &fixture.table_id,
            READER,
            &serde_json::json!({}),
        ))
        .await
        .expect("current UC response");
    assert_eq!(current.status(), StatusCode::OK);
    assert_eq!(
        table_read_header(&current, "X-Arco-Table-Read-Snapshot-Id"),
        initial_snapshot_id
    );
    assert_ne!(
        table_read_header(&current, "X-Arco-Table-Read-Permission-Watermark"),
        initial_watermark
    );
}

#[tokio::test]
async fn production_owner_path_accepts_rs256_bearer_without_debug_headers() {
    let backend = Arc::new(MemoryBackend::new());
    let fixture = seed_table_read_fixture(Arc::clone(&backend)).await;
    let router = production_rs256_router(backend, STORAGE_ROOT);
    let jwt = table_read_rs256_jwt(TENANT, WORKSPACE, READER);
    let request = table_read_bearer_request(&fixture.table_id, &jwt, &serde_json::json!({}));
    assert!(request.headers().get("X-Tenant-Id").is_none());
    assert!(request.headers().get("X-Workspace-Id").is_none());
    assert!(request.headers().get("X-User-Id").is_none());

    let response = router.oneshot(request).await.expect("RS256 UC response");

    assert_eq!(response.status(), StatusCode::OK);
}

fn production_router(backend: Arc<MemoryBackend>) -> Router {
    production_router_with_gate(backend, true)
}

fn production_router_with_gate(backend: Arc<MemoryBackend>, table_reads_enabled: bool) -> Router {
    let mut config = Config {
        debug: true,
        ..Config::default()
    };
    config.storage.bucket = Some(STORAGE_ROOT.to_string());
    config.unity_catalog.enabled = true;
    config.unity_catalog.table_reads_enabled = table_reads_enabled;

    ServerBuilder::new()
        .config(config)
        .storage_backend(backend)
        .build()
        .test_router()
}

fn production_rs256_router(backend: Arc<dyn StorageBackend>, storage_root: &str) -> Router {
    ServerBuilder::new()
        .config(production_rs256_config(storage_root))
        .storage_backend(backend)
        .build()
        .test_router()
}

fn production_rs256_config(storage_root: &str) -> Config {
    let mut config = Config {
        debug: false,
        posture: Posture::Private,
        ..Config::default()
    };
    config.storage.bucket = Some(storage_root.to_string());
    config.unity_catalog.enabled = true;
    config.unity_catalog.table_reads_enabled = true;
    config.jwt = JwtConfig {
        rs256_public_key_pem: Some(TEST_RSA_PUBLIC_KEY_PEM.to_string()),
        issuer: Some(JWT_ISSUER.to_string()),
        audience: Some(JWT_AUDIENCE.to_string()),
        ..JwtConfig::default()
    };

    config
}

fn table_read_request(table_id: &str, principal: &str, body: &Value) -> Request<Body> {
    table_read_request_in_scope(table_id, TENANT, WORKSPACE, principal, body)
}

fn table_descriptor_request(table_id: &str, principal: &str, body: &Value) -> Request<Body> {
    table_descriptor_request_in_scope(table_id, TENANT, WORKSPACE, principal, body)
}

fn table_descriptor_request_in_scope(
    table_id: &str,
    tenant: &str,
    workspace: &str,
    principal: &str,
    body: &Value,
) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!(
            "/api/2.1/unity-catalog/extensions/table-descriptors/{table_id}"
        ))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("X-User-Id", principal)
        .body(Body::from(body.to_string()))
        .expect("table-descriptor request")
}

fn table_read_request_in_scope(
    table_id: &str,
    tenant: &str,
    workspace: &str,
    principal: &str,
    body: &Value,
) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!(
            "/api/2.1/unity-catalog/extensions/table-reads/{table_id}"
        ))
        .header(header::CONTENT_TYPE, "application/json")
        .header("X-Tenant-Id", tenant)
        .header("X-Workspace-Id", workspace)
        .header("X-User-Id", principal)
        .body(Body::from(body.to_string()))
        .expect("table-read request")
}

fn table_read_bearer_request(table_id: &str, bearer: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!(
            "/api/2.1/unity-catalog/extensions/table-reads/{table_id}"
        ))
        .header(header::AUTHORIZATION, format!("Bearer {bearer}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .expect("RS256 table-read request")
}

fn table_read_header(response: &Response, name: &str) -> String {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .expect("expected a valid table-read descriptor header")
        .to_string()
}

async fn assert_uc_error(
    response: Response,
    expected_status: StatusCode,
    expected_code: &str,
    expected_message: Option<&str>,
) {
    assert_eq!(response.status(), expected_status);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("UC error body");
    let payload: Value = serde_json::from_slice(&body).expect("UC error response must be JSON");
    assert_eq!(
        payload.pointer("/error/error_code").and_then(Value::as_str),
        Some(expected_code)
    );
    if let Some(expected_message) = expected_message {
        assert_eq!(
            payload.pointer("/error/message").and_then(Value::as_str),
            Some(expected_message)
        );
    }
}

fn table_read_rs256_jwt(tenant: &str, workspace: &str, principal: &str) -> String {
    #[derive(Serialize)]
    struct Claims<'a> {
        tenant: &'a str,
        workspace: &'a str,
        sub: &'a str,
        iss: &'a str,
        aud: &'a str,
        exp: u64,
    }

    let exp = SystemTime::now()
        .checked_add(Duration::from_secs(60 * 60))
        .expect("JWT expiry")
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_secs();
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &Claims {
            tenant,
            workspace,
            sub: principal,
            iss: JWT_ISSUER,
            aud: JWT_AUDIENCE,
            exp,
        },
        &jsonwebtoken::EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY_PEM.as_bytes())
            .expect("RSA private test key"),
    )
    .expect("encode RS256 table-read JWT")
}

struct TableReadFixture {
    tenant: String,
    workspace: String,
    table_id: String,
    parquet: Bytes,
    storage: ScopedStorage,
}

async fn seed_table_read_fixture(backend: Arc<MemoryBackend>) -> TableReadFixture {
    seed_table_read_fixture_in_scope(
        backend,
        TENANT,
        WORKSPACE,
        None,
        "parquet",
        single_column_parquet(),
        true,
    )
    .await
}

async fn seed_table_read_fixture_in_scope(
    backend: Arc<MemoryBackend>,
    tenant: &str,
    workspace: &str,
    location: Option<String>,
    format: &str,
    parquet: Bytes,
    publish_projection: bool,
) -> TableReadFixture {
    let storage = ScopedStorage::new(backend, tenant, workspace).expect("scoped storage");
    let location = location.unwrap_or_else(|| {
        format!("{STORAGE_ROOT}/tenant={tenant}/workspace={workspace}/{OBJECT_PATH}")
    });

    let writer = table_read_catalog(&storage).await;
    let table = writer
        .register_table(
            RegisterTableRequest {
                namespace: "analytics".to_string(),
                name: "orders".to_string(),
                description: None,
                location: Some(location),
                format: Some(format.to_string()),
                columns: vec![revenue_column()],
            },
            WriteOptions::default(),
        )
        .await
        .expect("register parquet table");
    storage
        .put_raw(OBJECT_PATH, parquet.clone(), WritePrecondition::None)
        .await
        .expect("write scoped Parquet object");

    seed_table_read_authority(
        &storage,
        STORAGE_ROOT,
        &table.id,
        &table.name,
        READER,
        publish_projection,
    )
    .await;

    TableReadFixture {
        tenant: tenant.to_string(),
        workspace: workspace.to_string(),
        table_id: table.id,
        parquet,
        storage,
    }
}

async fn table_read_catalog(storage: &ScopedStorage) -> CatalogWriter {
    let compactor = Arc::new(Tier1Compactor::new(storage.clone()));
    let writer = CatalogWriter::new(storage.clone()).with_sync_compactor(compactor);
    writer.initialize().await.expect("initialize catalog");
    writer
        .create_namespace("analytics", None, WriteOptions::default())
        .await
        .expect("create namespace");
    writer
}

fn revenue_column() -> ColumnDefinition {
    ColumnDefinition {
        name: "revenue".to_string(),
        data_type: "INT64".to_string(),
        is_nullable: false,
        ordinal: 0,
        description: None,
    }
}

async fn seed_table_read_authority(
    storage: &ScopedStorage,
    storage_root: &str,
    table_id: &str,
    table_name: &str,
    reader: &str,
    publish_projection: bool,
) {
    let (tenant, workspace) = (storage.tenant_id(), storage.workspace_id());
    let scope = ControlPlaneScope::workspace_alias(tenant, workspace).expect("scope");
    let location_id = "location_table_read";
    let mutations = [
        (
            "table_read_principal_001",
            MetastoreMutation::PrincipalUpserted(PrincipalRecord {
                principal_id: reader.to_string(),
                name: reader.to_string(),
                principal_kind: PrincipalKind::User,
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_000,
                properties: BTreeMap::new(),
            }),
        ),
        (
            "table_read_table_002",
            MetastoreMutation::CatalogObjectUpserted(CatalogObjectRecord {
                object_id: table_id.to_string(),
                object_type: "TABLE".to_string(),
                qualified_name: format!("analytics.{table_name}"),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_001,
                properties: BTreeMap::new(),
            }),
        ),
        (
            "table_read_select_003",
            MetastoreMutation::GrantUpserted(GrantRecord {
                grant_id: "grant_table_read_select".to_string(),
                object_id: table_id.to_string(),
                object_type: "TABLE".to_string(),
                principal_id: reader.to_string(),
                privilege: "SELECT".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_002,
                properties: BTreeMap::new(),
                identity_cut: None,
            }),
        ),
        (
            "table_read_credential_004",
            MetastoreMutation::StorageCredentialUpserted(StorageCredentialRecord {
                credential_id: "credential_table_read".to_string(),
                name: "table-read-storage".to_string(),
                cloud: "s3".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_003,
                properties: BTreeMap::new(),
                secret_material_ref: None,
                encrypted_payload: None,
            }),
        ),
        (
            "table_read_location_005",
            MetastoreMutation::ExternalLocationUpserted(ExternalLocationRecord {
                location_id: location_id.to_string(),
                name: "table-read-root".to_string(),
                url: format!("{storage_root}/tenant={tenant}/workspace={workspace}/"),
                credential_id: "credential_table_read".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_004,
                properties: BTreeMap::new(),
            }),
        ),
        (
            "table_read_binding_006",
            MetastoreMutation::WorkspaceBindingUpserted(WorkspaceBindingRecord {
                binding_id: "binding_table_read".to_string(),
                workspace_id: workspace.to_string(),
                object_id: location_id.to_string(),
                object_type: "EXTERNAL_LOCATION".to_string(),
                owner: "owner".to_string(),
                lifecycle_state: LifecycleState::Active,
                updated_at_ms: 1_800_000_000_005,
                properties: BTreeMap::new(),
            }),
        ),
    ];
    let ledger = MetastoreLedger::new(storage.clone()).expect("metastore ledger");
    for (index, (name, mutation)) in mutations.into_iter().enumerate() {
        let sequence = u64::try_from(index + 1).unwrap();
        let event = MetastoreEvent::new_scoped(&scope, name, sequence, mutation);
        ledger
            .append_event(&event)
            .await
            .expect("append metastore event");
    }
    if publish_projection {
        publish_current_metastore_projection(storage, &ProjectionRegistry::default())
            .await
            .expect("publish storage-governance projection");
    }
}

async fn revoke_fixture_select(fixture: &TableReadFixture, principal: &str) {
    let scope = ControlPlaneScope::workspace_alias(&fixture.tenant, &fixture.workspace)
        .expect("fixture scope");
    let ledger = MetastoreLedger::new(fixture.storage.clone()).expect("metastore ledger");
    let sequence = next_fixture_sequence(&ledger).await;
    let event = MetastoreEvent::new_scoped(
        &scope,
        format!("table_read_revoke_{sequence}"),
        sequence,
        MetastoreMutation::GrantUpserted(GrantRecord {
            grant_id: "grant_table_read_select".to_string(),
            object_id: fixture.table_id.clone(),
            object_type: "TABLE".to_string(),
            principal_id: principal.to_string(),
            privilege: "SELECT".to_string(),
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Deleted,
            updated_at_ms: 1_800_000_000_100,
            properties: BTreeMap::new(),
            identity_cut: None,
        }),
    );
    ledger
        .append_event(&event)
        .await
        .expect("append revoke event");
    publish_current_metastore_projection(&fixture.storage, &ProjectionRegistry::default())
        .await
        .expect("publish revoked projection");
}

async fn set_fixture_modify(fixture: &TableReadFixture, lifecycle_state: LifecycleState) {
    let scope = ControlPlaneScope::workspace_alias(&fixture.tenant, &fixture.workspace)
        .expect("fixture scope");
    let ledger = MetastoreLedger::new(fixture.storage.clone()).expect("metastore ledger");
    let sequence = next_fixture_sequence(&ledger).await;
    ledger
        .append_event(&MetastoreEvent::new_scoped(
            &scope,
            format!("table_write_modify_{sequence}"),
            sequence,
            MetastoreMutation::GrantUpserted(GrantRecord {
                grant_id: "grant_table_write_modify".to_string(),
                object_id: fixture.table_id.clone(),
                object_type: "TABLE".to_string(),
                principal_id: READER.to_string(),
                privilege: "MODIFY".to_string(),
                owner: "owner".to_string(),
                lifecycle_state,
                updated_at_ms: 1_800_000_000_000 + sequence.cast_signed(),
                properties: BTreeMap::new(),
                identity_cut: None,
            }),
        ))
        .await
        .expect("append table MODIFY update");
}

async fn fixture_object_versions(fixture: &TableReadFixture) -> BTreeMap<String, String> {
    fixture
        .storage
        .list_meta("")
        .await
        .expect("list scoped objects")
        .into_iter()
        .map(|object| (object.path.as_str().to_string(), object.version))
        .collect()
}

async fn append_unrelated_principal(fixture: &TableReadFixture) {
    append_unrelated_principal_with_publication(fixture, true).await;
}

async fn append_unrelated_principal_with_publication(
    fixture: &TableReadFixture,
    publish_projection: bool,
) {
    let scope = ControlPlaneScope::workspace_alias(&fixture.tenant, &fixture.workspace)
        .expect("fixture scope");
    let ledger = MetastoreLedger::new(fixture.storage.clone()).expect("metastore ledger");
    let sequence = next_fixture_sequence(&ledger).await;
    let event = MetastoreEvent::new_scoped(
        &scope,
        format!("table_read_unrelated_principal_{sequence}"),
        sequence,
        MetastoreMutation::PrincipalUpserted(PrincipalRecord {
            principal_id: "user_unrelated".to_string(),
            name: "user_unrelated".to_string(),
            principal_kind: PrincipalKind::User,
            owner: "owner".to_string(),
            lifecycle_state: LifecycleState::Active,
            updated_at_ms: 1_800_000_000_101,
            properties: BTreeMap::new(),
        }),
    );
    ledger
        .append_event(&event)
        .await
        .expect("append unrelated principal event");
    if publish_projection {
        publish_current_metastore_projection(&fixture.storage, &ProjectionRegistry::default())
            .await
            .expect("publish updated projection");
    }
}

async fn next_fixture_sequence(ledger: &MetastoreLedger) -> u64 {
    ledger
        .latest_watermark()
        .await
        .expect("read latest fixture watermark")
        .map_or(1, |watermark| {
            watermark
                .sequence
                .checked_add(1)
                .expect("sequence overflow")
        })
}

async fn replace_fixture_object(fixture: &TableReadFixture, bytes: Bytes) {
    let metadata = fixture
        .storage
        .head_raw(OBJECT_PATH)
        .await
        .expect("read fixture object metadata")
        .expect("fixture object exists");
    assert!(matches!(
        fixture
            .storage
            .put_raw(
                OBJECT_PATH,
                bytes,
                WritePrecondition::MatchesVersion(metadata.version),
            )
            .await
            .expect("replace fixture object"),
        WriteResult::Success { .. }
    ));
}

fn single_column_parquet() -> Bytes {
    single_column_parquet_with_value(42)
}

fn single_column_parquet_with_value(value: i64) -> Bytes {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "revenue",
        DataType::Int64,
        false,
    )]));
    let revenue: ArrayRef = Arc::new(Int64Array::from(vec![value]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![revenue]).expect("record batch");
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).expect("Parquet writer");
    writer.write(&batch).expect("write Parquet batch");
    Bytes::from(writer.into_inner().expect("finish Parquet writer"))
}

async fn managed_delta_fixture(
    backend: Arc<dyn StorageBackend>,
    storage_root: &str,
    workspace: &str,
) -> (TableReadFixture, Router, DeltaPaths) {
    let storage = ScopedStorage::new(Arc::clone(&backend), TENANT, workspace).unwrap();
    let writer = table_read_catalog(&storage).await;
    let columns = [
        ("day", "date"),
        ("amount_cents", "long"),
        ("status", "string"),
    ]
    .into_iter()
    .enumerate()
    .map(|(ordinal, (name, data_type))| ColumnDefinition {
        name: name.to_string(),
        data_type: data_type.to_string(),
        is_nullable: false,
        ordinal: i32::try_from(ordinal).unwrap(),
        description: None,
    })
    .collect();
    let table = writer
        .register_table_in_schema(
            "default",
            "analytics",
            RegisterTableInSchemaRequest {
                name: "orders".to_string(),
                description: Some("Paid orders in signed USD cents.".to_string()),
                location: None,
                format: Some("delta".to_string()),
                table_type: Some("MANAGED".to_string()),
                properties: Some(BTreeMap::from([
                    (
                        "arco.dataset.grain".to_string(),
                        "one row per order".to_string(),
                    ),
                    (
                        "arco.dataset.units".to_string(),
                        r#"{"amount_cents":"USD cents"}"#.to_string(),
                    ),
                ])),
                columns,
            },
            WriteOptions::default(),
        )
        .await
        .expect("register managed orders");
    seed_table_read_authority(
        &storage,
        storage_root,
        &table.id,
        &table.name,
        READER,
        false,
    )
    .await;
    let paths = DeltaPaths::legacy(Uuid::parse_str(&table.id).unwrap());
    let fixture = TableReadFixture {
        tenant: TENANT.to_string(),
        workspace: workspace.to_string(),
        table_id: table.id,
        parquet: Bytes::new(),
        storage,
    };
    set_fixture_modify(&fixture, LifecycleState::Active).await;
    publish_current_metastore_projection(&fixture.storage, &ProjectionRegistry::default())
        .await
        .expect("publish managed Delta fixture authority");
    (
        fixture,
        production_rs256_router(backend, storage_root),
        paths,
    )
}

async fn delta_orders_commit(
    fixture: &TableReadFixture,
    paths: &DeltaPaths,
    version: i64,
    amounts: &[i64],
    protocol: Option<Value>,
) -> String {
    delta_fixture_commit(
        &fixture.storage,
        paths,
        version,
        orders_parquet(amounts),
        &[
            ("day", "date"),
            ("amount_cents", "long"),
            ("status", "string"),
        ],
        protocol,
    )
    .await
}

async fn delta_fixture_commit(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    version: i64,
    parquet: Bytes,
    fields: &[(&str, &str)],
    protocol: Option<Value>,
) -> String {
    let file = format!("orders-{version}.parquet");
    assert!(matches!(
        storage
            .put_raw(
                &format!("{}/{}", paths.table_root(), file),
                parquet.clone(),
                WritePrecondition::DoesNotExist,
            )
            .await
            .unwrap(),
        WriteResult::Success { .. },
    ));
    let mut actions = Vec::new();
    actions.push(json!({"commitInfo": {"operation": "WRITE"}}));
    if version == 0 {
        actions.push(json!({"protocol": protocol.unwrap_or_else(|| json!({
            "minReaderVersion": 1, "minWriterVersion": 2
        }))}));
        let fields = fields.iter().map(|(name, data_type)| {
                json!({"name": name, "type": data_type, "nullable": false, "metadata": {}})
            }).collect::<Vec<_>>();
        actions.push(json!({"metaData": {
            "id": "d97c39b0-72c5-49b8-9f49-28fe5c9e5cee",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json!({"type": "struct", "fields": fields}).to_string(),
            "partitionColumns": [], "configuration": {},
        }}));
    }
    actions.push(json!({"add": {
        "path": file, "partitionValues": {}, "size": parquet.len(), "modificationTime": 0,
        "dataChange": true, "tags": {"arco.content.sha256": hex::encode(Sha256::digest(&parquet))},
    }}));
    let mut payload = String::new();
    for action in actions {
        writeln!(payload, "{action}").expect("serialize log action");
    }
    payload
}

fn orders_parquet(amounts: &[i64]) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("day", DataType::Date32, false),
        Field::new("amount_cents", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    fixture_parquet(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![20_454; amounts.len()])),
            Arc::new(Int64Array::from(amounts.to_vec())),
            Arc::new(StringArray::from(vec!["paid"; amounts.len()])),
        ],
    )
}

fn fixture_parquet(schema: Arc<Schema>, arrays: Vec<ArrayRef>) -> Bytes {
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    Bytes::from(writer.into_inner().unwrap())
}

async fn commit_delta(router: &Router, table_id: &str, token: &str, version: i64, log: &str) {
    let path = format!("/api/v1/delta/tables/{table_id}/commits");
    let (status, staged) = post_json(
        router,
        &format!("{path}/stage"),
        token,
        json!({"payload": log}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{staged}");
    let (status, committed) = post_json(
        router,
        &path,
        token,
        json!({
            "read_version": version - 1,
            "staged_path": staged.get("staged_path").expect("staged path"),
            "staged_version": staged.get("staged_version").expect("staged version"),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{committed}");
    assert_eq!(
        committed.get("version").and_then(Value::as_i64),
        Some(version)
    );
}

fn extension_path(extension: &str, table_id: &str) -> String {
    format!("/api/2.1/unity-catalog/extensions/{extension}/{table_id}")
}

async fn post_json(router: &Router, path: &str, token: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::post(path)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("Idempotency-Key", Uuid::now_v7().to_string())
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    if (path.contains("/extensions/")
        || path.starts_with("/api/v1/event-streams/")
        || path == "/api/v1/managed-delta/ensure")
        && status.is_success()
    {
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    }
    let bytes = to_bytes(response.into_body(), 12 * 1024 * 1024)
        .await
        .unwrap();
    let body = if bytes.is_empty() && !status.is_success() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).expect("JSON API response")
    };
    (status, body)
}
