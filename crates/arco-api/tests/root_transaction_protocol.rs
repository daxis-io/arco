#![allow(clippy::expect_used)]
#![allow(missing_docs)]

#[path = "../../arco-core/tests/support/spy_backend.rs"]
mod spy_backend;
#[path = "support/control_plane_transactions.rs"]
mod support;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::http::StatusCode;

use arco_core::control_plane_transactions::ControlPlaneTxDomain;
use arco_core::storage::StorageBackend;
use arco_proto::arco::controlplane::v1::CommitRootTransactionResponse;
use spy_backend::{SpyBackend, SpyOp};
use support::{
    FailPrefixOnNoneBackend, TENANT, WORKSPACE, load_idempotency_record, load_root_tx_record,
    post_error_json, post_protobuf, root_request, test_router_with_backend,
};

fn scoped_path(path: &str) -> String {
    format!("tenant={TENANT}/workspace={WORKSPACE}/{path}")
}

#[tokio::test]
async fn replay_repairs_missing_root_tx_record_from_visible_idempotency_without_listing()
-> Result<()> {
    let inner: Arc<dyn StorageBackend> = Arc::new(FailPrefixOnNoneBackend::new(
        scoped_path("transactions/root/"),
        1,
    ));
    let spy = Arc::new(SpyBackend::new(inner));
    spy.set_fail_on_list(true);
    let backend: Arc<dyn StorageBackend> = spy.clone();
    let router = test_router_with_backend(backend.clone());

    let first = root_request(
        "idem-root-repair-tx-01",
        "req-root-repair-tx-01",
        "repair-root-tx",
        "run-root-repair-tx-01",
    );
    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/commitRootTransaction",
        &first,
        "idem-root-repair-tx-01",
        "req-root-repair-tx-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let cached = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Root,
        "idem-root-repair-tx-01",
    )
    .await?;
    assert!(cached.visible_at.is_some());
    assert!(cached.tx_record.is_some());

    let replay = root_request(
        "idem-root-repair-tx-01",
        "req-root-repair-tx-02",
        "repair-root-tx",
        "run-root-repair-tx-01",
    );
    let (_status, response): (_, CommitRootTransactionResponse) = post_protobuf(
        router,
        "/api/v1/transactions/commitRootTransaction",
        &replay,
        "idem-root-repair-tx-01",
        "req-root-repair-tx-02",
    )
    .await?;

    let receipt = response.receipt.context("root replay receipt missing")?;
    assert_eq!(receipt.tx_id, cached.tx_id);

    let stored = load_root_tx_record(backend.clone(), &receipt.tx_id).await?;
    assert_eq!(stored.tx_id, receipt.tx_id);
    assert_eq!(stored.visible_at, cached.visible_at);
    assert!(
        spy.ops().iter().all(|op| !matches!(op, SpyOp::List { .. })),
        "root replay must not call list(): {:?}",
        spy.ops()
    );

    Ok(())
}

#[tokio::test]
async fn replay_repairs_missing_visible_idempotency_from_root_tx_record_without_listing()
-> Result<()> {
    let inner: Arc<dyn StorageBackend> = Arc::new(FailPrefixOnNoneBackend::new(
        scoped_path("transactions/idempotency/root/"),
        1,
    ));
    let spy = Arc::new(SpyBackend::new(inner));
    spy.set_fail_on_list(true);
    let backend: Arc<dyn StorageBackend> = spy.clone();
    let router = test_router_with_backend(backend.clone());

    let first = root_request(
        "idem-root-repair-idem-01",
        "req-root-repair-idem-01",
        "repair-root-idem",
        "run-root-repair-idem-01",
    );
    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/commitRootTransaction",
        &first,
        "idem-root-repair-idem-01",
        "req-root-repair-idem-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let initial_idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Root,
        "idem-root-repair-idem-01",
    )
    .await?;
    assert!(initial_idem.visible_at.is_none());
    assert!(initial_idem.tx_record.is_none());
    let visible_root = load_root_tx_record(backend.clone(), &initial_idem.tx_id).await?;
    assert_eq!(visible_root.tx_id, initial_idem.tx_id);
    assert!(visible_root.visible_at.is_some());

    let replay = root_request(
        "idem-root-repair-idem-01",
        "req-root-repair-idem-02",
        "repair-root-idem",
        "run-root-repair-idem-01",
    );
    let (_status, response): (_, CommitRootTransactionResponse) = post_protobuf(
        router,
        "/api/v1/transactions/commitRootTransaction",
        &replay,
        "idem-root-repair-idem-01",
        "req-root-repair-idem-02",
    )
    .await?;

    let receipt = response.receipt.context("root replay receipt missing")?;
    let repaired_idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Root,
        "idem-root-repair-idem-01",
    )
    .await?;
    assert_eq!(repaired_idem.tx_id, receipt.tx_id);
    assert!(repaired_idem.visible_at.is_some());
    assert!(repaired_idem.tx_record.is_some());
    assert!(
        spy.ops().iter().all(|op| !matches!(op, SpyOp::List { .. })),
        "root replay must not call list(): {:?}",
        spy.ops()
    );

    Ok(())
}
