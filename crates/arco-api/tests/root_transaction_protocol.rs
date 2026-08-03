#![allow(clippy::expect_used)]
#![allow(missing_docs)]

#[path = "../../arco-core/tests/support/spy_backend.rs"]
mod spy_backend;
#[path = "support/control_plane_transactions.rs"]
mod support;

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;

use arco_core::control_plane_transactions::{ControlPlaneTxDomain, ControlPlaneTxPaths};
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
use arco_proto::arco::controlplane::v1::CommitRootTransactionResponse;
use spy_backend::{SpyBackend, SpyOp};
use support::{
    TENANT, WORKSPACE, load_idempotency_record, load_root_tx_record, post_protobuf, root_request,
    test_router_with_backend,
};

fn scoped_path(path: &str) -> String {
    format!("tenant={TENANT}/workspace={WORKSPACE}/{path}")
}

#[tokio::test]
async fn replay_repairs_missing_root_tx_record_from_visible_idempotency_without_listing()
-> Result<()> {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(inner));
    let backend: Arc<dyn StorageBackend> = spy.clone();
    let router = test_router_with_backend(backend.clone());

    let first = root_request(
        "idem-root-repair-tx-01",
        "req-root-repair-tx-01",
        "repair-root-tx",
        "run-root-repair-tx-01",
    );
    let (_status, response): (_, CommitRootTransactionResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/commitRootTransaction",
        &first,
        "idem-root-repair-tx-01",
        "req-root-repair-tx-01",
    )
    .await?;
    let first_receipt = response.receipt.context("initial root receipt missing")?;

    let cached = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Root,
        "idem-root-repair-tx-01",
    )
    .await?;
    assert!(cached.visible_at.is_some());
    assert!(cached.tx_record.is_some());
    assert_eq!(cached.tx_id, first_receipt.tx_id);
    backend
        .delete(&scoped_path(&ControlPlaneTxPaths::record(
            ControlPlaneTxDomain::Root,
            &cached.tx_id,
        )))
        .await?;

    let replay = root_request(
        "idem-root-repair-tx-01",
        "req-root-repair-tx-02",
        "repair-root-tx",
        "run-root-repair-tx-01",
    );
    spy.clear_ops();
    spy.set_fail_on_list(true);
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
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(inner));
    let backend: Arc<dyn StorageBackend> = spy.clone();
    let router = test_router_with_backend(backend.clone());

    let first = root_request(
        "idem-root-repair-idem-01",
        "req-root-repair-idem-01",
        "repair-root-idem",
        "run-root-repair-idem-01",
    );
    let (_status, response): (_, CommitRootTransactionResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/commitRootTransaction",
        &first,
        "idem-root-repair-idem-01",
        "req-root-repair-idem-01",
    )
    .await?;
    let first_receipt = response.receipt.context("initial root receipt missing")?;

    let mut initial_idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Root,
        "idem-root-repair-idem-01",
    )
    .await?;
    assert!(initial_idem.visible_at.is_some());
    assert!(initial_idem.tx_record.is_some());
    assert_eq!(initial_idem.tx_id, first_receipt.tx_id);
    let visible_root = load_root_tx_record(backend.clone(), &initial_idem.tx_id).await?;
    assert_eq!(visible_root.tx_id, initial_idem.tx_id);
    assert!(visible_root.visible_at.is_some());
    initial_idem.visible_at = None;
    initial_idem.tx_record = None;
    backend
        .put(
            &scoped_path(&ControlPlaneTxPaths::idempotency(
                ControlPlaneTxDomain::Root,
                "idem-root-repair-idem-01",
            )),
            Bytes::from(serde_json::to_vec(&initial_idem)?),
            WritePrecondition::None,
        )
        .await?;

    let replay = root_request(
        "idem-root-repair-idem-01",
        "req-root-repair-idem-02",
        "repair-root-idem",
        "run-root-repair-idem-01",
    );
    spy.clear_ops();
    spy.set_fail_on_list(true);
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
