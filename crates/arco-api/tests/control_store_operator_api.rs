//! Operator-only control-store endpoint contracts.
//!
//! These endpoints live in `arco-api` because platform IAM makes this service
//! the sole writer of the `state-store/` object prefix. They were previously
//! mounted on `arco-compactor`, whose service account has no such grant.

use std::sync::Arc;

use arco_api::config::{Config, Posture};
use arco_api::server::Server;
use arco_catalog::ArcoStateTxn;
use arco_catalog::state_store::{
    ControlMvpProjectionOutboxRecord, ControlMvpStateStore, StateScope, TxnOptions,
};
use arco_core::ScopedStorage;
use arco_core::storage::{MemoryBackend, StorageBackend};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use serde_json::Value;
use tower::ServiceExt;

const TENANT: &str = "acme";
const WORKSPACE: &str = "analytics";
const SOURCE_DOMAIN: &str = "phase5-source";
const OUTBOX_PATH: &str = "/internal/control-store/projection-outbox";

fn config(operator_endpoints: bool) -> Config {
    let mut config = Config {
        debug: true,
        posture: Posture::Dev,
        ..Config::default()
    };
    config.control_store_operator_endpoints = operator_endpoints;
    config
}

fn router_with(backend: Arc<dyn StorageBackend>, operator_endpoints: bool) -> axum::Router {
    Server::with_storage_backend(config(operator_endpoints), backend).test_router()
}

fn scoped(backend: Arc<dyn StorageBackend>) -> ScopedStorage {
    ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage")
}

fn post(body: &'static str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(OUTBOX_PATH)
        .header("content-type", "application/json")
        .header("X-Tenant-Id", TENANT)
        .header("X-Workspace-Id", WORKSPACE)
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
    let scope = StateScope::new(TENANT, WORKSPACE, SOURCE_DOMAIN);
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
    .expect("stage outbox record");
    txn.commit().await.expect("commit source record");
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
