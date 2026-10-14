//! Operator-only control-store endpoint contracts.
//!
//! These endpoints live in `arco-api` because platform IAM makes this service
//! the sole writer of the `control/` object prefix. They were previously
//! mounted on `arco-compactor`, whose service account has no such grant.

#![allow(
    clippy::expect_used,
    reason = "operator API contract setup must fail immediately when a fixture or request is invalid"
)]

use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arco_api::config::{Config, ControlV1CatalogRootConfig, ControlV1CursorKeyConfig, Posture};
use arco_api::server::Server;
use arco_catalog::state_store::projection_outbox_acks::ProjectionOutboxAckWriter;
use arco_catalog::state_store::{
    ControlMvpProjectionOutboxRecord, ControlMvpStateStore, StateScope, TxnOptions,
};
use arco_catalog::{
    ArcoStateTxn, CatalogProjectionMaterializer, CatalogWriter, ControlCatalogAuthority,
    Tier1Compactor, WriteOptions,
};
use arco_core::ScopedStorage;
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_test_utils::http_signed_url::HttpSignedUrlBackend;
use arco_test_utils::storage::TracingMemoryBackend;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tower::ServiceExt;

const TENANT: &str = "acme";
const WORKSPACE: &str = "analytics";
const SOURCE_DOMAIN: &str = "phase5-source";
const OUTBOX_PATH: &str = "/internal/control-store/projection-outbox";
const OPERATOR_GROUP: &str = "group:control-store-operators";
const URL_PATH: &str = "/internal/control-store/catalog-projection/urls";

#[derive(Debug, Default)]
struct ReplaceOnSecondHead {
    inner: Arc<MemoryBackend>,
    target: Mutex<Option<String>>,
    target_heads: AtomicUsize,
}

impl ReplaceOnSecondHead {
    fn arm(&self, path: String) {
        *self.target.lock().expect("race target lock") = Some(path);
        self.target_heads.store(0, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl StorageBackend for ReplaceOnSecondHead {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        let initial = self.inner.head(path).await?;
        if self.target.lock().expect("race target lock").as_deref() == Some(path)
            && self.target_heads.fetch_add(1, Ordering::SeqCst) == 1
        {
            let size = initial.as_ref().map_or(0, |meta| {
                usize::try_from(meta.size).expect("test object size fits usize")
            });
            self.inner
                .put(path, Bytes::from(vec![b'x'; size]), WritePrecondition::None)
                .await?;
            return self.inner.head(path).await;
        }
        Ok(initial)
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn url_request(body: &'static str, group: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(URL_PATH)
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .header("X-Groups", group)
        .body(Body::from(body))
        .expect("URL request")
}

fn config(operator_endpoints: bool) -> Config {
    let mut config = Config {
        debug: true,
        posture: Posture::Dev,
        catalog_control_v1_root: Some(ControlV1CatalogRootConfig {
            tenant_id: TENANT.to_string(),
            workspace_id: WORKSPACE.to_string(),
        }),
        catalog_control_v1_cursor_key: Some(ControlV1CursorKeyConfig::new(
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        )),
        ..Config::default()
    };
    config.control_store_operator_endpoints = operator_endpoints;
    config.control_store_operator_group = operator_endpoints.then(|| OPERATOR_GROUP.to_string());
    config
}

fn router_with(backend: Arc<dyn StorageBackend>, operator_endpoints: bool) -> axum::Router {
    Server::with_storage_backend(config(operator_endpoints), backend).test_router()
}

fn scoped(backend: Arc<dyn StorageBackend>) -> ScopedStorage {
    ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage")
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one lifecycle proves the pinned read, HTTP/Parquet consumption, lag and quarantine refusals"
)]
async fn operator_catalog_urls_pin_one_control_projection_and_refuse_lag() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let signer = Arc::new(
        HttpSignedUrlBackend::new(inner)
            .await
            .expect("signed URL backend"),
    );
    let backend: Arc<dyn StorageBackend> = signer;
    let authority = ControlCatalogAuthority::new(
        scoped(Arc::clone(&backend)),
        StateScope::new(TENANT, WORKSPACE, "catalog"),
    )
    .expect("authority");
    authority
        .create_catalog("first", None, WriteOptions::default())
        .await
        .expect("first catalog");
    let router = router_with(Arc::clone(&backend), true);
    assert_eq!(
        StatusCode::FORBIDDEN,
        router
            .clone()
            .oneshot(url_request("", "ordinary"))
            .await
            .expect("request")
            .status()
    );
    assert_eq!(
        StatusCode::BAD_REQUEST,
        router
            .clone()
            .oneshot(url_request(r#"{"path":"commits.parquet"}"#, OPERATOR_GROUP))
            .await
            .expect("request")
            .status()
    );
    assert_eq!(
        StatusCode::SERVICE_UNAVAILABLE,
        router
            .clone()
            .oneshot(url_request("", OPERATOR_GROUP))
            .await
            .expect("request")
            .status()
    );
    let drain = router
        .clone()
        .oneshot(post(
            r#"{"sourceDomain":"catalog","consumerId":"catalog-parquet-v1","drain":true}"#,
        ))
        .await
        .expect("drain");
    assert_eq!(StatusCode::OK, drain.status());
    let first = json_body(
        router
            .clone()
            .oneshot(url_request("", OPERATOR_GROUP))
            .await
            .expect("request"),
    )
    .await;
    assert_eq!(1, first["descriptorVersion"]);
    assert_eq!(TENANT, first["scope"]["tenantId"]);
    assert_eq!(WORKSPACE, first["scope"]["workspaceId"]);
    assert_eq!("catalog", first["scope"]["domain"]);
    assert_eq!("controlV1", first["authority"]["kind"]);
    assert_eq!(
        first["authority"]["sequence"],
        first["projection"]["sequence"]
    );
    assert!(first["authority"]["manifestId"].as_str().is_some());
    assert_eq!(
        64,
        first["projection"]["manifestChecksumSha256"]
            .as_str()
            .expect("manifest checksum")
            .len()
    );
    assert_eq!(900, first["ttlSeconds"]);
    let files = first["files"].as_array().expect("files");
    assert_eq!(4, files.len());
    assert!(files.iter().all(|file| {
        !file["path"]
            .as_str()
            .unwrap_or_default()
            .contains("commits")
    }));
    let url = files[0]["url"].as_str().expect("signed URL");
    let parsed_url = reqwest::Url::parse(url).expect("signed URL syntax");
    let expires: u64 = parsed_url
        .query_pairs()
        .find(|(name, _)| name == "expires")
        .expect("signed expiry")
        .1
        .parse()
        .expect("expiry timestamp");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    assert!((899..=900).contains(&expires.saturating_sub(now)));
    assert!(
        reqwest::Client::new()
            .head(url)
            .send()
            .await
            .expect("HEAD")
            .status()
            .is_success()
    );
    let ranged = reqwest::Client::new()
        .get(url)
        .header("Range", "bytes=0-3")
        .send()
        .await
        .expect("HTTP range");
    assert_eq!(StatusCode::PARTIAL_CONTENT, ranged.status());
    let parquet_bytes = reqwest::get(url)
        .await
        .expect("independent HTTP GET")
        .bytes()
        .await
        .expect("Parquet bytes");
    assert_eq!(
        files[0]["checksumSha256"],
        hex::encode(Sha256::digest(&parquet_bytes))
    );
    let reader = SerializedFileReader::new(parquet_bytes).expect("independent Parquet reader");
    assert!(reader.metadata().num_row_groups() > 0);

    authority
        .create_catalog("second", None, WriteOptions::default())
        .await
        .expect("second catalog");
    assert_eq!(
        StatusCode::SERVICE_UNAVAILABLE,
        router
            .clone()
            .oneshot(url_request("", OPERATOR_GROUP))
            .await
            .expect("lagging request")
            .status()
    );
    assert!(
        reqwest::Client::new()
            .head(url)
            .send()
            .await
            .expect("pinned HEAD")
            .status()
            .is_success()
    );
    let drain = router
        .clone()
        .oneshot(post(
            r#"{"sourceDomain":"catalog","consumerId":"catalog-parquet-v1","drain":true}"#,
        ))
        .await
        .expect("second drain");
    assert_eq!(StatusCode::OK, drain.status());
    let second = json_body(
        router
            .clone()
            .oneshot(url_request("", OPERATOR_GROUP))
            .await
            .expect("second request"),
    )
    .await;
    assert_ne!(
        first["projection"]["manifestChecksumSha256"],
        second["projection"]["manifestChecksumSha256"]
    );
    assert_ne!(
        first["authority"]["manifestId"],
        second["authority"]["manifestId"]
    );

    let status_writer = ProjectionOutboxAckWriter::new(
        scoped(Arc::clone(&backend)),
        StateScope::new(TENANT, WORKSPACE, "projection-outbox-acks"),
    )
    .expect("status writer");
    let now = chrono::Utc::now().timestamp_millis();
    status_writer
        .record_projection_failure("catalog-parquet-v1", 3, "SYNTHETIC_FAILURE", true, now)
        .await
        .expect("failure status");
    assert_eq!(
        StatusCode::SERVICE_UNAVAILABLE,
        router
            .clone()
            .oneshot(url_request("", OPERATOR_GROUP))
            .await
            .expect("failed projection")
            .status()
    );
    status_writer
        .record_projection_quarantine(
            "catalog-parquet-v1",
            4,
            "synthetic-record",
            "SYNTHETIC_QUARANTINE",
            now + 1,
        )
        .await
        .expect("quarantine status");
    assert_eq!(
        StatusCode::SERVICE_UNAVAILABLE,
        router
            .oneshot(url_request("", OPERATOR_GROUP))
            .await
            .expect("quarantined projection")
            .status()
    );
}

#[tokio::test]
async fn operator_catalog_urls_read_legacy_immutable_manifest() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let signer = Arc::new(
        HttpSignedUrlBackend::new(inner)
            .await
            .expect("signed URL backend"),
    );
    let backend: Arc<dyn StorageBackend> = signer;
    let storage = scoped(Arc::clone(&backend));
    let writer = CatalogWriter::new(storage.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(storage)));
    writer.initialize().await.expect("initialize");
    writer
        .create_catalog("first", None, WriteOptions::default())
        .await
        .expect("catalog");
    let mut legacy = config(true);
    legacy.catalog_control_v1_root = None;
    legacy.catalog_control_v1_cursor_key = None;
    let response = Server::with_storage_backend(legacy, backend)
        .test_router()
        .oneshot(url_request("", OPERATOR_GROUP))
        .await
        .expect("request");
    assert_eq!(StatusCode::OK, response.status());
    let json = json_body(response).await;
    assert_eq!(4, json["files"].as_array().expect("files").len());
    assert_eq!(1, json["descriptorVersion"]);
    assert_eq!("legacy", json["authority"]["kind"]);
    assert!(json["authority"]["manifestId"].as_str().is_some());
    assert!(json["projection"]["manifestPath"].as_str().is_some());
}

#[tokio::test]
async fn operator_catalog_urls_refuse_missing_or_mismatched_projection_objects() {
    let inner = Arc::new(MemoryBackend::new());
    let raw: Arc<dyn StorageBackend> = inner.clone();
    let backend: Arc<dyn StorageBackend> = Arc::new(
        HttpSignedUrlBackend::new(raw)
            .await
            .expect("signed URL backend"),
    );
    ControlCatalogAuthority::new(
        scoped(Arc::clone(&backend)),
        StateScope::new(TENANT, WORKSPACE, "catalog"),
    )
    .expect("authority")
    .create_catalog("first", None, WriteOptions::default())
    .await
    .expect("catalog");
    let router = router_with(Arc::clone(&backend), true);
    assert_eq!(
        StatusCode::OK,
        router
            .clone()
            .oneshot(post(
                r#"{"sourceDomain":"catalog","consumerId":"catalog-parquet-v1","drain":true}"#,
            ))
            .await
            .expect("drain")
            .status()
    );
    let objects = inner
        .list("tenant=acme/workspace=analytics/control/v1/projections/catalog-parquet/")
        .await
        .expect("projection objects");
    let table_path = objects
        .iter()
        .find(|object| object.path.ends_with("/tables.parquet"))
        .expect("tables object")
        .path
        .clone();
    inner
        .put(
            &table_path,
            Bytes::from_static(b"interrupted replacement"),
            WritePrecondition::None,
        )
        .await
        .expect("replace table object");
    assert_eq!(
        StatusCode::SERVICE_UNAVAILABLE,
        router
            .clone()
            .oneshot(url_request("", OPERATOR_GROUP))
            .await
            .expect("mismatched object")
            .status()
    );
    inner
        .delete(&table_path)
        .await
        .expect("remove table object");
    assert_eq!(
        StatusCode::SERVICE_UNAVAILABLE,
        router
            .oneshot(url_request("", OPERATOR_GROUP))
            .await
            .expect("missing object")
            .status()
    );
}

#[tokio::test]
async fn operator_catalog_urls_refuse_object_replaced_during_descriptor_issue() {
    let racing = Arc::new(ReplaceOnSecondHead::default());
    let raw: Arc<dyn StorageBackend> = racing.clone();
    let backend: Arc<dyn StorageBackend> = Arc::new(
        HttpSignedUrlBackend::new(raw)
            .await
            .expect("signed URL backend"),
    );
    ControlCatalogAuthority::new(
        scoped(Arc::clone(&backend)),
        StateScope::new(TENANT, WORKSPACE, "catalog"),
    )
    .expect("authority")
    .create_catalog("first", None, WriteOptions::default())
    .await
    .expect("catalog");
    let router = router_with(Arc::clone(&backend), true);
    assert_eq!(
        StatusCode::OK,
        router
            .clone()
            .oneshot(post(
                r#"{"sourceDomain":"catalog","consumerId":"catalog-parquet-v1","drain":true}"#,
            ))
            .await
            .expect("drain")
            .status()
    );
    let tables_path = racing
        .list("tenant=acme/workspace=analytics/control/v1/projections/catalog-parquet/")
        .await
        .expect("projection objects")
        .into_iter()
        .find(|object| object.path.ends_with("/tables.parquet"))
        .expect("tables object")
        .path;
    racing.arm(tables_path);
    assert_eq!(
        StatusCode::SERVICE_UNAVAILABLE,
        router
            .oneshot(url_request("", OPERATOR_GROUP))
            .await
            .expect("racing request")
            .status()
    );
}

#[tokio::test]
async fn operator_catalog_urls_fail_when_backend_cannot_sign() {
    let signer = Arc::new(TracingMemoryBackend::new());
    let backend: Arc<dyn StorageBackend> = signer.clone();
    let storage = scoped(Arc::clone(&backend));
    let writer = CatalogWriter::new(storage.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(storage)));
    writer.initialize().await.expect("initialize");
    writer
        .create_catalog("first", None, WriteOptions::default())
        .await
        .expect("catalog");
    let catalogs_path = signer
        .list("tenant=acme/workspace=analytics/")
        .await
        .expect("snapshot listing")
        .into_iter()
        .find(|object| object.path.ends_with("/catalogs.parquet"))
        .expect("catalog projection")
        .path;
    signer.inject_failure(catalogs_path);
    let mut legacy = config(true);
    legacy.catalog_control_v1_root = None;
    legacy.catalog_control_v1_cursor_key = None;
    let response = Server::with_storage_backend(legacy, backend)
        .test_router()
        .oneshot(url_request("", OPERATOR_GROUP))
        .await
        .expect("request");
    assert!(response.status().is_server_error());
}

#[tokio::test]
async fn operator_catalog_urls_use_verified_jwt_scope() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend: Arc<dyn StorageBackend> = Arc::new(
        HttpSignedUrlBackend::new(inner)
            .await
            .expect("signed URL backend"),
    );
    let storage = scoped(Arc::clone(&backend));
    let writer = CatalogWriter::new(storage.clone())
        .with_sync_compactor(Arc::new(Tier1Compactor::new(storage)));
    writer.initialize().await.expect("initialize");
    writer
        .create_catalog("first", None, WriteOptions::default())
        .await
        .expect("catalog");
    let mut release = config(true);
    release.debug = false;
    release.posture = Posture::Private;
    release.catalog_control_v1_root = None;
    release.catalog_control_v1_cursor_key = None;
    release.jwt.hs256_secret = Some("test-jwt-secret".to_string());
    release.jwt.issuer = Some("test-issuer".to_string());
    release.jwt.audience = Some("test-audience".to_string());
    let router = Server::with_storage_backend(release, backend).test_router();
    let token = |workspace: &str, groups: Vec<&str>| {
        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &serde_json::json!({
                "tenant": TENANT, "workspace": workspace, "sub": "operator",
                "groups": groups, "iss": "test-issuer", "aud": "test-audience",
                "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
            }),
            &jsonwebtoken::EncodingKey::from_secret(b"test-jwt-secret"),
        )
        .expect("JWT")
    };
    for (workspace, groups, expected) in [
        (WORKSPACE, vec![OPERATOR_GROUP], StatusCode::OK),
        (
            "other",
            vec![OPERATOR_GROUP],
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (WORKSPACE, vec!["ordinary"], StatusCode::FORBIDDEN),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(URL_PATH)
                    .header(
                        "Authorization",
                        format!("Bearer {}", token(workspace, groups)),
                    )
                    .header("X-Tenant-Id", TENANT)
                    .header("X-Workspace-Id", WORKSPACE)
                    .header("X-Groups", OPERATOR_GROUP)
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(expected, response.status());
    }
}

fn post(body: &'static str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(OUTBOX_PATH)
        .header("content-type", "application/json")
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
        .header("X-Groups", OPERATOR_GROUP)
        .body(Body::from(body))
        .expect("request build failed")
}

async fn json_body(response: axum::response::Response) -> Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&body).expect("json body")
}

/// Seeds one committed source record carrying a staged outbox entry, in the
/// request scope the operator endpoint will derive from the request context.
async fn seed_source_record(backend: Arc<dyn StorageBackend>) {
    seed_source_record_in_domain(backend, SOURCE_DOMAIN).await;
}

async fn seed_source_record_in_domain(backend: Arc<dyn StorageBackend>, domain: &str) {
    let scope = StateScope::new(TENANT, WORKSPACE, domain);
    let store = ControlMvpStateStore::new(scoped(backend), scope.clone()).expect("control store");
    let mut txn = store
        .begin_control_txn(TxnOptions::new(Some(scope)))
        .await
        .expect("begin txn");
    txn.put(b"row/record-1", Bytes::from_static(b"{}"))
        .await
        .expect("stage row");
    txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "record-1",
        Bytes::from_static(b"{}"),
    ))
    .await
    .expect("stage outbox record");
    txn.commit().await.expect("commit source record");
}

#[tokio::test]
async fn catalog_projection_outbox_materializes_before_operator_drain_acknowledges() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    ControlCatalogAuthority::new(
        scoped(Arc::clone(&backend)),
        StateScope::new(TENANT, WORKSPACE, "catalog"),
    )
    .expect("catalog authority")
    .create_catalog("analytics", None, WriteOptions::default())
    .await
    .expect("catalog mutation");

    let response = router_with(Arc::clone(&backend), true)
        .oneshot(post(
            r#"{"sourceDomain":"catalog","consumerId":"catalog-parquet-v1","drain":true}"#,
        ))
        .await
        .expect("request failed");
    assert_eq!(StatusCode::OK, response.status());
    let json = json_body(response).await;
    assert!(
        json["drain"]["drainedRecordIds"]
            .as_array()
            .is_some_and(|records| records.len() == 1),
        "unexpected body: {json}"
    );
    let artifacts = backend
        .list("tenant=acme/workspace=analytics/control/v1/projections/catalog-parquet/")
        .await
        .expect("projection artifacts");
    assert!(
        artifacts
            .iter()
            .any(|object| object.path.ends_with("manifest.json")),
        "acknowledged drain must leave a materialized manifest: {artifacts:?}"
    );

    let status = CatalogProjectionMaterializer::new(scoped(Arc::clone(&backend)))
        .expect("catalog materializer")
        .status()
        .await
        .expect("projection status read")
        .expect("projection status exists");
    assert_eq!(Some(1), status.applied_authority_sequence());
    assert_eq!(Some(1), status.observed_authority_sequence());
    assert!(status.last_success_at_ms().is_some());
    assert!(status.failure_state().is_none());

    for body in [
        r#"{"sourceDomain":"catalog","consumerId":"catalog-parquet-v1","trim":true}"#,
        r#"{"sourceDomain":"catalog","consumerId":"catalog-parquet-v1","forceRebindConsumer":true}"#,
    ] {
        let response = router_with(Arc::clone(&backend), true)
            .oneshot(post(body))
            .await
            .expect("catalog source mutation request");
        assert_eq!(StatusCode::BAD_REQUEST, response.status());
    }
}

#[tokio::test]
async fn control_store_endpoints_absent_unless_enabled_and_drain_trim_work_when_enabled() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    // Disabled (default): the route does not exist.
    let response = router_with(Arc::clone(&backend), false)
        .oneshot(post(
            r#"{"sourceDomain":"phase5-source","consumerId":"consumer-a"}"#,
        ))
        .await
        .expect("request failed");
    assert_eq!(StatusCode::NOT_FOUND, response.status());

    seed_source_record(Arc::clone(&backend)).await;

    // Enabled: drain + trim through the operator endpoint.
    let response = router_with(Arc::clone(&backend), true)
        .oneshot(post(
            r#"{"sourceDomain":"phase5-source","consumerId":"consumer-a","drain":true,"trim":true}"#,
        ))
        .await
        .expect("request failed");
    assert_eq!(StatusCode::OK, response.status());
    let json = json_body(response).await;
    assert_eq!(
        serde_json::json!(["record-1"]),
        json["drain"]["drainedRecordIds"],
        "unexpected body: {json}"
    );
    assert_eq!(
        serde_json::json!(["record-1"]),
        json["trim"]["trimmedRecordIds"],
        "unexpected body: {json}"
    );
    assert_eq!(
        serde_json::json!(["evt-00000000000000000001-record-1"]),
        json["trim"]["trimmedEventIds"],
        "the operator surface reports the immutable event identity it trimmed: {json}"
    );
    assert!(
        json["backlog"]["pendingRecordIds"]
            .as_array()
            .is_some_and(Vec::is_empty),
        "unexpected body: {json}"
    );
    // The trim commit itself advances the source-domain sequence past the
    // consumer's last acknowledged record, so ack-derived freshness honestly
    // reports staleness while the pending backlog stays empty.
    assert!(
        json["freshness"]
            .as_str()
            .is_some_and(|value| value.contains("StaleProjection")),
        "unexpected body: {json}"
    );
}

#[tokio::test]
async fn control_store_outbox_endpoint_enforces_consumer_binding_and_force_rebind() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    seed_source_record(Arc::clone(&backend)).await;
    let router = router_with(Arc::clone(&backend), true);

    // consumer-a's first drain registers the single-consumer binding.
    let response = router
        .clone()
        .oneshot(post(
            r#"{"sourceDomain":"phase5-source","consumerId":"consumer-a","drain":true}"#,
        ))
        .await
        .expect("request failed");
    assert_eq!(StatusCode::OK, response.status());

    // A different consumer fails closed with the typed conflict and a rebind
    // hint.
    let response = router
        .clone()
        .oneshot(post(
            r#"{"sourceDomain":"phase5-source","consumerId":"consumer-b","drain":true}"#,
        ))
        .await
        .expect("request failed");
    assert_eq!(StatusCode::PRECONDITION_FAILED, response.status());
    let json = json_body(response).await;
    assert!(
        json["message"]
            .as_str()
            .is_some_and(|message| message.contains("consumer-a")),
        "unexpected body: {json}"
    );
    assert!(
        json["details"]["hint"]
            .as_str()
            .is_some_and(|hint| hint.contains("forceRebindConsumer")),
        "unexpected body: {json}"
    );

    // A deliberate force rebind reports the previous binding, mints a new
    // tenure, and transfers drain authority.
    let response = router
        .clone()
        .oneshot(post(
            r#"{"sourceDomain":"phase5-source","consumerId":"consumer-b","forceRebindConsumer":true,"drain":true}"#,
        ))
        .await
        .expect("request failed");
    assert_eq!(StatusCode::OK, response.status());
    let json = json_body(response).await;
    assert_eq!(
        serde_json::json!("consumer-a"),
        json["rebind"]["previousConsumer"],
        "unexpected body: {json}"
    );
    assert_eq!(
        serde_json::json!(2),
        json["rebind"]["incarnation"],
        "a transfer must mint a new binding incarnation: {json}"
    );
    assert_eq!(
        serde_json::json!(["record-1"]),
        json["drain"]["drainedRecordIds"],
        "the new tenure must redeliver rather than inherit the old tenure's acks: {json}"
    );
}

#[tokio::test]
async fn control_store_outbox_endpoint_refuses_unclaimed_and_unusable_writer_epochs() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    seed_source_record(Arc::clone(&backend)).await;
    let router = router_with(Arc::clone(&backend), true);

    // u64::MAX is refused before any work happens: publishing it would wedge
    // the claim protocol permanently.
    let response = router
        .clone()
        .oneshot(post(
            r#"{"sourceDomain":"phase5-source","consumerId":"consumer-a","writerEpoch":18446744073709551615,"drain":true}"#,
        ))
        .await
        .expect("request failed");
    assert_eq!(StatusCode::BAD_REQUEST, response.status());

    // An unclaimed future epoch is refused with the operator hint explaining
    // that only a claim advances the published epoch.
    let response = router
        .oneshot(post(
            r#"{"sourceDomain":"phase5-source","consumerId":"consumer-a","writerEpoch":7,"drain":true}"#,
        ))
        .await
        .expect("request failed");
    assert_eq!(StatusCode::PRECONDITION_FAILED, response.status());
    let json = json_body(response).await;
    assert!(
        json["details"]["hint"]
            .as_str()
            .is_some_and(|hint| hint.contains("writerEpoch must equal the published pointer epoch")),
        "unexpected body: {json}"
    );
}

#[tokio::test]
async fn control_store_endpoints_require_authentication_and_are_never_mounted_in_public_posture() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    seed_source_record(Arc::clone(&backend)).await;

    // Enabling the routes without configuring an operator group grants
    // nobody access, even when the caller presents some authenticated group.
    let mut unconfigured = config(true);
    unconfigured.control_store_operator_group = None;
    let response = Server::with_storage_backend(unconfigured, Arc::clone(&backend))
        .test_router()
        .oneshot(post(
            r#"{"sourceDomain":"phase5-source","consumerId":"consumer-a","drain":true}"#,
        ))
        .await
        .expect("request failed");
    assert_eq!(StatusCode::FORBIDDEN, response.status());
    let json = json_body(response).await;
    assert!(
        json["message"]
            .as_str()
            .is_some_and(|message| message.contains("no operator authority is configured")),
        "unexpected body: {json}"
    );

    // Authentication is still insufficient when the verified groups claim
    // does not carry the configured operator authority.
    let response = router_with(Arc::clone(&backend), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(OUTBOX_PATH)
                .header("content-type", "application/json")
                .header("X-Tenant-Id", TENANT)
                .header("X-Workspace-Id", WORKSPACE)
                .header("X-Groups", "group:ordinary-tenant")
                .body(Body::from(
                    r#"{"sourceDomain":"phase5-source","consumerId":"consumer-a","drain":true}"#,
                ))
                .expect("request build failed"),
        )
        .await
        .expect("request failed");
    assert_eq!(StatusCode::FORBIDDEN, response.status());

    // No verified scope, no operation: the endpoint derives the tenant and
    // workspace it acts on from authentication, never from the request body.
    let response = router_with(Arc::clone(&backend), true)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(OUTBOX_PATH)
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"sourceDomain":"phase5-source","consumerId":"consumer-a","drain":true}"#,
                ))
                .expect("request build failed"),
        )
        .await
        .expect("request failed");
    assert_eq!(StatusCode::UNAUTHORIZED, response.status());

    // A public posture never mounts an operator surface, even when the flag is
    // set, mirroring how /metrics is withheld there.
    let mut public = config(true);
    public.debug = false;
    public.posture = Posture::Public;
    let response = Server::with_storage_backend(public, Arc::clone(&backend))
        .test_router()
        .oneshot(post(
            r#"{"sourceDomain":"phase5-source","consumerId":"consumer-a","drain":true}"#,
        ))
        .await
        .expect("request failed");
    assert_eq!(StatusCode::NOT_FOUND, response.status());
}

#[tokio::test]
async fn shadow_import_endpoint_is_gated_and_reports_classified_comparisons() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let shadow_import = |enabled: bool| {
        let backend = Arc::clone(&backend);
        async move {
            router_with(backend, enabled)
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/internal/control-store/shadow-import")
                        .header("X-Tenant-Id", TENANT)
                        .header("X-Workspace-Id", WORKSPACE)
                        .header("X-Groups", OPERATOR_GROUP)
                        .body(Body::empty())
                        .expect("request build failed"),
                )
                .await
                .expect("request failed")
        }
    };

    // Disabled: the route does not exist, so axum answers with an empty 404.
    let absent = shadow_import(false).await;
    assert_eq!(StatusCode::NOT_FOUND, absent.status());
    let absent_body = axum::body::to_bytes(absent.into_body(), usize::MAX)
        .await
        .expect("body");
    assert!(
        absent_body.is_empty(),
        "an unmounted route must not answer with a handler body"
    );

    // Enabled: the handler runs. With no published catalog manifest there is
    // nothing to import, and it reports that as a typed API error rather than
    // pretending the comparison ran.
    let present = shadow_import(true).await;
    let json = json_body(present).await;
    assert!(
        json["code"].is_string(),
        "the mounted route must answer with the typed error contract: {json}"
    );
    assert!(
        json["message"]
            .as_str()
            .is_some_and(|message| message.contains("manifest")),
        "an unimportable workspace must say what was missing: {json}"
    );
}
