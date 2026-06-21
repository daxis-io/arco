#![allow(clippy::expect_used)]
#![allow(missing_docs)]

#[path = "../../arco-core/tests/support/spy_backend.rs"]
mod spy_backend;
#[path = "support/control_plane_transactions.rs"]
mod support;

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::http::StatusCode;

use arco_core::ControlPlaneTxDomain;
use arco_core::catalog_paths::{CatalogDomain, CatalogPaths};
use arco_core::control_plane_transactions::ControlPlaneTxStatus;
use arco_core::storage::StorageBackend;
use arco_flow::orchestration_manifest_pointer_path;
use arco_proto::arco::controlplane::v1::{
    ApplyCatalogDdlResponse, CommitOrchestrationBatchResponse,
};
use spy_backend::{SpyBackend, SpyOp};
use support::{
    FailPrefixOnNoneBackend, FailReadsAfterTriggerPutBackend, TENANT, WORKSPACE,
    catalog_create_default_schema_request, load_catalog_tx_record, load_idempotency_record,
    load_orchestration_tx_record, orchestration_request, post_error_json, post_protobuf,
    test_router_with_backend,
};

fn scoped_path(path: &str) -> String {
    format!("tenant={TENANT}/workspace={WORKSPACE}/{path}")
}

#[tokio::test]
async fn apply_catalog_ddl_replays_from_cached_visible_record_after_finalize_write_failure()
-> Result<()> {
    let fail_prefix = format!("tenant={TENANT}/workspace={WORKSPACE}/transactions/catalog/");
    let backend: Arc<dyn StorageBackend> = Arc::new(FailPrefixOnNoneBackend::new(fail_prefix, 1));
    let router = test_router_with_backend(backend.clone());

    let first = catalog_create_default_schema_request(
        "idem-cat-finalize-cache-01",
        "req-cat-finalize-cache-01",
        "cached-visible",
    );
    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &first,
        "idem-cat-finalize-cache-01",
        "req-cat-finalize-cache-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Catalog,
        "idem-cat-finalize-cache-01",
    )
    .await?;
    assert!(idem.visible_at.is_some());
    assert!(idem.tx_record.is_some());

    let replay = catalog_create_default_schema_request(
        "idem-cat-finalize-cache-01",
        "req-cat-finalize-cache-02",
        "cached-visible",
    );
    let (_status, response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &replay,
        "idem-cat-finalize-cache-01",
        "req-cat-finalize-cache-02",
    )
    .await?;

    let receipt = response
        .receipt
        .context("catalog cached replay receipt missing")?;
    assert_eq!(receipt.tx_id, idem.tx_id);

    let stored = load_catalog_tx_record(backend, &receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert_eq!(
        stored.result.as_ref().map(|result| result.tx_id.as_str()),
        Some(receipt.tx_id.as_str())
    );

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_keeps_visible_success_when_pointer_readback_fails() -> Result<()> {
    let pointer_path = format!(
        "tenant={TENANT}/workspace={WORKSPACE}/{}",
        CatalogPaths::domain_manifest_pointer(CatalogDomain::Catalog)
    );
    let backend: Arc<dyn StorageBackend> = Arc::new(FailReadsAfterTriggerPutBackend::new(
        pointer_path.clone(),
        3,
        vec![pointer_path],
        Vec::new(),
        2,
    ));
    let router = test_router_with_backend(backend.clone());

    let first = catalog_create_default_schema_request(
        "idem-cat-visible-boundary-01",
        "req-cat-visible-boundary-01",
        "visible-boundary",
    );
    let (_status, first_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &first,
        "idem-cat-visible-boundary-01",
        "req-cat-visible-boundary-01",
    )
    .await?;

    let first_receipt = first_response
        .receipt
        .context("catalog receipt missing after pointer readback failure")?;
    let stored = load_catalog_tx_record(backend.clone(), &first_receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert_eq!(
        stored.result.as_ref().map(|result| result.tx_id.as_str()),
        Some(first_receipt.tx_id.as_str())
    );

    let replay = catalog_create_default_schema_request(
        "idem-cat-visible-boundary-01",
        "req-cat-visible-boundary-02",
        "visible-boundary",
    );
    let (_status, replay_response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &replay,
        "idem-cat-visible-boundary-01",
        "req-cat-visible-boundary-02",
    )
    .await?;

    let replay_receipt = replay_response
        .receipt
        .context("catalog replay receipt missing after pointer readback failure")?;
    assert_eq!(replay_receipt.tx_id, first_receipt.tx_id);
    assert_eq!(replay_receipt.commit_id, first_receipt.commit_id);

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_repairs_missing_catalog_tx_record_from_visible_idempotency_without_listing()
-> Result<()> {
    let inner: Arc<dyn StorageBackend> = Arc::new(FailPrefixOnNoneBackend::new(
        scoped_path("transactions/catalog/"),
        1,
    ));
    let spy = Arc::new(SpyBackend::new(inner));
    let backend: Arc<dyn StorageBackend> = spy.clone();
    let router = test_router_with_backend(backend.clone());

    let first = catalog_create_default_schema_request(
        "idem-cat-repair-tx-01",
        "req-cat-repair-tx-01",
        "repair-catalog-tx",
    );
    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &first,
        "idem-cat-repair-tx-01",
        "req-cat-repair-tx-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let cached = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Catalog,
        "idem-cat-repair-tx-01",
    )
    .await?;
    assert!(cached.visible_at.is_some());
    assert!(cached.tx_record.is_some());

    let replay = catalog_create_default_schema_request(
        "idem-cat-repair-tx-01",
        "req-cat-repair-tx-02",
        "repair-catalog-tx",
    );
    spy.clear_ops();
    spy.set_fail_on_list(true);
    let (_status, response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &replay,
        "idem-cat-repair-tx-01",
        "req-cat-repair-tx-02",
    )
    .await?;

    let receipt = response.receipt.context("catalog replay receipt missing")?;
    assert_eq!(receipt.tx_id, cached.tx_id);

    let stored = load_catalog_tx_record(backend.clone(), &receipt.tx_id).await?;
    assert_eq!(stored.tx_id, receipt.tx_id);
    assert_eq!(stored.visible_at, cached.visible_at);
    assert!(
        spy.ops().iter().all(|op| !matches!(op, SpyOp::List { .. })),
        "catalog replay must not call list(): {:?}",
        spy.ops()
    );

    Ok(())
}

#[tokio::test]
async fn apply_catalog_ddl_repairs_missing_visible_idempotency_from_catalog_tx_record_without_listing()
-> Result<()> {
    let inner: Arc<dyn StorageBackend> = Arc::new(FailPrefixOnNoneBackend::new(
        scoped_path("transactions/idempotency/catalog/"),
        1,
    ));
    let spy = Arc::new(SpyBackend::new(inner));
    let backend: Arc<dyn StorageBackend> = spy.clone();
    let router = test_router_with_backend(backend.clone());

    let first = catalog_create_default_schema_request(
        "idem-cat-repair-idem-01",
        "req-cat-repair-idem-01",
        "repair-catalog-idem",
    );
    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/applyCatalogDdl",
        &first,
        "idem-cat-repair-idem-01",
        "req-cat-repair-idem-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let initial_idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Catalog,
        "idem-cat-repair-idem-01",
    )
    .await?;
    assert!(initial_idem.visible_at.is_none());
    assert!(initial_idem.tx_record.is_none());

    let visible_record = load_catalog_tx_record(backend.clone(), &initial_idem.tx_id).await?;
    assert_eq!(visible_record.tx_id, initial_idem.tx_id);
    assert!(visible_record.visible_at.is_some());

    let replay = catalog_create_default_schema_request(
        "idem-cat-repair-idem-01",
        "req-cat-repair-idem-02",
        "repair-catalog-idem",
    );
    spy.clear_ops();
    spy.set_fail_on_list(true);
    let (_status, response): (_, ApplyCatalogDdlResponse) = post_protobuf(
        router,
        "/api/v1/transactions/applyCatalogDdl",
        &replay,
        "idem-cat-repair-idem-01",
        "req-cat-repair-idem-02",
    )
    .await?;

    let receipt = response.receipt.context("catalog replay receipt missing")?;
    let repaired_idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Catalog,
        "idem-cat-repair-idem-01",
    )
    .await?;
    assert_eq!(repaired_idem.tx_id, receipt.tx_id);
    assert!(repaired_idem.visible_at.is_some());
    assert!(repaired_idem.tx_record.is_some());
    assert!(
        spy.ops().iter().all(|op| !matches!(op, SpyOp::List { .. })),
        "catalog replay must not call list(): {:?}",
        spy.ops()
    );

    Ok(())
}

#[tokio::test]
async fn commit_orchestration_batch_keeps_visible_success_when_pointer_readback_fails() -> Result<()>
{
    let pointer_path = format!(
        "tenant={TENANT}/workspace={WORKSPACE}/{}",
        orchestration_manifest_pointer_path()
    );
    let backend: Arc<dyn StorageBackend> = Arc::new(FailReadsAfterTriggerPutBackend::new(
        pointer_path.clone(),
        1,
        vec![pointer_path.clone()],
        vec![pointer_path],
        2,
    ));
    let router = test_router_with_backend(backend.clone());

    let first = orchestration_request(
        "idem-orch-visible-boundary-01",
        "req-orch-visible-boundary-01",
        "run-visible-boundary-01",
    );
    let (_status, first_response): (_, CommitOrchestrationBatchResponse) = post_protobuf(
        router.clone(),
        "/api/v1/transactions/commitOrchestrationBatch",
        &first,
        "idem-orch-visible-boundary-01",
        "req-orch-visible-boundary-01",
    )
    .await?;

    let first_receipt = first_response
        .receipt
        .context("orchestration receipt missing after pointer readback failure")?;
    let stored = load_orchestration_tx_record(backend.clone(), &first_receipt.tx_id).await?;
    assert_eq!(stored.status, ControlPlaneTxStatus::Visible);
    assert_eq!(
        stored.result.as_ref().map(|result| result.tx_id.as_str()),
        Some(first_receipt.tx_id.as_str())
    );

    let replay = orchestration_request(
        "idem-orch-visible-boundary-01",
        "req-orch-visible-boundary-02",
        "run-visible-boundary-01",
    );
    let (_status, replay_response): (_, CommitOrchestrationBatchResponse) = post_protobuf(
        router,
        "/api/v1/transactions/commitOrchestrationBatch",
        &replay,
        "idem-orch-visible-boundary-01",
        "req-orch-visible-boundary-02",
    )
    .await?;

    let replay_receipt = replay_response
        .receipt
        .context("orchestration replay receipt missing after pointer readback failure")?;
    assert_eq!(replay_receipt.tx_id, first_receipt.tx_id);
    assert_eq!(replay_receipt.commit_id, first_receipt.commit_id);

    Ok(())
}

#[tokio::test]
async fn commit_orchestration_batch_repairs_missing_visible_idempotency_from_tx_record_without_listing()
-> Result<()> {
    let inner: Arc<dyn StorageBackend> = Arc::new(FailPrefixOnNoneBackend::new(
        scoped_path("transactions/idempotency/orchestration/"),
        1,
    ));
    let spy = Arc::new(SpyBackend::new(inner));
    spy.set_fail_on_list(true);
    let backend: Arc<dyn StorageBackend> = spy.clone();
    let router = test_router_with_backend(backend.clone());

    let first = orchestration_request(
        "idem-orch-repair-idem-01",
        "req-orch-repair-idem-01",
        "run-orch-repair-idem-01",
    );
    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/commitOrchestrationBatch",
        &first,
        "idem-orch-repair-idem-01",
        "req-orch-repair-idem-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let initial_idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Orchestration,
        "idem-orch-repair-idem-01",
    )
    .await?;
    assert!(initial_idem.visible_at.is_none());
    assert!(initial_idem.tx_record.is_none());

    let visible_record = load_orchestration_tx_record(backend.clone(), &initial_idem.tx_id).await?;
    assert_eq!(visible_record.tx_id, initial_idem.tx_id);
    assert!(visible_record.visible_at.is_some());

    let replay = orchestration_request(
        "idem-orch-repair-idem-01",
        "req-orch-repair-idem-02",
        "run-orch-repair-idem-01",
    );
    let (_status, response): (_, CommitOrchestrationBatchResponse) = post_protobuf(
        router,
        "/api/v1/transactions/commitOrchestrationBatch",
        &replay,
        "idem-orch-repair-idem-01",
        "req-orch-repair-idem-02",
    )
    .await?;

    let receipt = response
        .receipt
        .context("orchestration replay receipt missing")?;
    let repaired_idem = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Orchestration,
        "idem-orch-repair-idem-01",
    )
    .await?;
    assert_eq!(repaired_idem.tx_id, receipt.tx_id);
    assert!(repaired_idem.visible_at.is_some());
    assert!(repaired_idem.tx_record.is_some());
    assert!(
        spy.ops().iter().all(|op| !matches!(op, SpyOp::List { .. })),
        "orchestration replay must not call list(): {:?}",
        spy.ops()
    );

    Ok(())
}

#[tokio::test]
async fn commit_orchestration_batch_repairs_missing_orchestration_tx_record_from_visible_idempotency_without_listing()
-> Result<()> {
    let inner: Arc<dyn StorageBackend> = Arc::new(FailPrefixOnNoneBackend::new(
        scoped_path("transactions/orchestration/"),
        1,
    ));
    let spy = Arc::new(SpyBackend::new(inner));
    spy.set_fail_on_list(true);
    let backend: Arc<dyn StorageBackend> = spy.clone();
    let router = test_router_with_backend(backend.clone());

    let first = orchestration_request(
        "idem-orch-repair-tx-01",
        "req-orch-repair-tx-01",
        "run-orch-repair-tx-01",
    );
    let (status, _) = post_error_json(
        router.clone(),
        "/api/v1/transactions/commitOrchestrationBatch",
        &first,
        "idem-orch-repair-tx-01",
        "req-orch-repair-tx-01",
    )
    .await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let cached = load_idempotency_record(
        backend.clone(),
        ControlPlaneTxDomain::Orchestration,
        "idem-orch-repair-tx-01",
    )
    .await?;
    assert!(cached.visible_at.is_some());
    assert!(cached.tx_record.is_some());

    let replay = orchestration_request(
        "idem-orch-repair-tx-01",
        "req-orch-repair-tx-02",
        "run-orch-repair-tx-01",
    );
    let (_status, response): (_, CommitOrchestrationBatchResponse) = post_protobuf(
        router,
        "/api/v1/transactions/commitOrchestrationBatch",
        &replay,
        "idem-orch-repair-tx-01",
        "req-orch-repair-tx-02",
    )
    .await?;

    let receipt = response
        .receipt
        .context("orchestration replay receipt missing")?;
    assert_eq!(receipt.tx_id, cached.tx_id);

    let stored = load_orchestration_tx_record(backend.clone(), &receipt.tx_id).await?;
    assert_eq!(stored.tx_id, receipt.tx_id);
    assert_eq!(stored.visible_at, cached.visible_at);
    assert!(
        spy.ops().iter().all(|op| !matches!(op, SpyOp::List { .. })),
        "orchestration replay must not call list(): {:?}",
        spy.ops()
    );

    Ok(())
}
