#![allow(clippy::expect_used)]
#![allow(missing_docs)]

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
use support::{
    FailPrefixOnNoneBackend, FailReadsAfterTriggerPutBackend, TENANT, WORKSPACE,
    catalog_create_namespace_request, load_catalog_tx_record, load_idempotency_record,
    load_orchestration_tx_record, orchestration_request, post_error_json, post_protobuf,
    test_router_with_backend,
};

#[tokio::test]
async fn apply_catalog_ddl_replays_from_cached_visible_record_after_finalize_write_failure()
-> Result<()> {
    let fail_prefix = format!("tenant={TENANT}/workspace={WORKSPACE}/transactions/catalog/");
    let backend: Arc<dyn StorageBackend> = Arc::new(FailPrefixOnNoneBackend::new(fail_prefix, 1));
    let router = test_router_with_backend(backend.clone());

    let first = catalog_create_namespace_request(
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

    let replay = catalog_create_namespace_request(
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

    let first = catalog_create_namespace_request(
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

    let replay = catalog_create_namespace_request(
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
