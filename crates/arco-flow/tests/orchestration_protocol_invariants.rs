#![allow(clippy::expect_used)]
#![allow(missing_docs)]

#[path = "../../arco-core/tests/support/spy_backend.rs"]
mod spy_backend;

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;

use arco_core::ScopedStorage;
use arco_core::control_plane_transactions::{
    ControlPlaneTxDomain, ControlPlaneTxKind, ControlPlaneTxPaths, ControlPlaneTxStatus,
    DomainCommit, RootTxManifest, RootTxManifestDomain, RootTxReceipt, RootTxRecord,
};
use arco_core::storage::{MemoryBackend, StorageBackend, WritePrecondition};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::{
    MicroCompactor, OrchestrationManifest, OrchestrationManifestPointer,
};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TriggerInfo};
use arco_flow::orchestration_manifest_pointer_path;
use bytes::Bytes;
use chrono::Utc;
use spy_backend::{SpyBackend, SpyOp};

const TENANT: &str = "test-tenant";
const WORKSPACE: &str = "test-workspace";
type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn scoped_storage<B>(backend: Arc<B>) -> ScopedStorage
where
    B: StorageBackend,
{
    ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage")
}

fn run_triggered_event(run_id: &str) -> OrchestrationEvent {
    OrchestrationEvent::new(
        TENANT,
        WORKSPACE,
        OrchestrationEventData::RunTriggered {
            run_id: run_id.to_string(),
            plan_id: format!("plan-{run_id}"),
            trigger: TriggerInfo::Manual {
                user_id: "tester".to_string(),
            },
            root_assets: vec![],
            run_key: None,
            labels: std::collections::HashMap::new(),
            code_version: None,
        },
    )
}

async fn append_and_compact(
    storage: &ScopedStorage,
    compactor: &MicroCompactor,
    event: OrchestrationEvent,
) -> TestResult<arco_flow::orchestration::compactor::CompactionResult> {
    let path = LedgerWriter::event_path(&event);
    LedgerWriter::new(storage.clone()).append(event).await?;
    Ok(compactor.compact_events(vec![path]).await?)
}

async fn current_orchestration_head(
    storage: &ScopedStorage,
) -> TestResult<(OrchestrationManifestPointer, OrchestrationManifest)> {
    let pointer_bytes = storage
        .get_raw(orchestration_manifest_pointer_path())
        .await?;
    let pointer: OrchestrationManifestPointer = serde_json::from_slice(&pointer_bytes)?;
    let manifest_bytes = storage.get_raw(&pointer.manifest_path).await?;
    let manifest: OrchestrationManifest = serde_json::from_slice(&manifest_bytes)?;
    Ok((pointer, manifest))
}

async fn seed_root_token_for_orchestration(
    storage: &ScopedStorage,
    tx_id: &str,
    pointer: &OrchestrationManifestPointer,
    result: &arco_flow::orchestration::compactor::CompactionResult,
) -> TestResult {
    let visible_at = Utc::now();
    let super_manifest_path = ControlPlaneTxPaths::root_super_manifest(tx_id);
    let root_manifest = RootTxManifest {
        tx_id: tx_id.to_string(),
        fencing_token: 1,
        published_at: visible_at,
        domains: BTreeMap::from([(
            ControlPlaneTxDomain::Orchestration,
            RootTxManifestDomain {
                manifest_id: pointer.manifest_id.clone(),
                manifest_path: pointer.manifest_path.clone(),
                commit_id: result.manifest_revision.clone(),
            },
        )]),
    };
    storage
        .put_raw(
            &super_manifest_path,
            Bytes::from(serde_json::to_vec(&root_manifest)?),
            WritePrecondition::DoesNotExist,
        )
        .await?;

    let root_record = RootTxRecord {
        tx_id: tx_id.to_string(),
        kind: ControlPlaneTxKind::RootCommit,
        status: ControlPlaneTxStatus::Visible,
        repair_pending: false,
        request_id: "req-orch-root-token".to_string(),
        idempotency_key: "idem-orch-root-token".to_string(),
        request_hash: "sha256:orch-root-token".to_string(),
        lock_path: ControlPlaneTxPaths::root_lock(),
        fencing_token: 1,
        prepared_at: visible_at,
        visible_at: Some(visible_at),
        durable_append: None,
        result: Some(RootTxReceipt {
            tx_id: tx_id.to_string(),
            root_commit_id: "01JFLOWROOTTOKENCOMMIT00000001".to_string(),
            super_manifest_path: super_manifest_path.clone(),
            domain_commits: vec![DomainCommit {
                domain: ControlPlaneTxDomain::Orchestration,
                tx_id: "01JFLOWROOTPARTICIPANT0000001".to_string(),
                commit_id: result.manifest_revision.clone(),
                manifest_id: pointer.manifest_id.clone(),
                manifest_path: pointer.manifest_path.clone(),
                read_token: format!("orchestration:{}", pointer.manifest_id),
            }],
            read_token: format!("root:{tx_id}"),
            visible_at,
        }),
    };
    storage
        .put_raw(
            &ControlPlaneTxPaths::record(ControlPlaneTxDomain::Root, tx_id),
            Bytes::from(serde_json::to_vec(&root_record)?),
            WritePrecondition::DoesNotExist,
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn ordinary_orchestration_reads_never_list_or_read_ledger() -> TestResult {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(inner));
    let storage = scoped_storage(spy.clone());
    let compactor = MicroCompactor::new(storage.clone());
    append_and_compact(&storage, &compactor, run_triggered_event("run-visible-01")).await?;

    spy.clear_ops();
    spy.set_fail_on_list(true);
    spy.add_fail_get_prefix(format!(
        "tenant={TENANT}/workspace={WORKSPACE}/ledger/orchestration/"
    ));

    let (_manifest, state) = compactor.load_state().await?;
    assert!(state.runs.contains_key("run-visible-01"));
    assert!(
        spy.ops().iter().all(|op| !matches!(op, SpyOp::List { .. })),
        "ordinary orchestration reads must not call list(): {:?}",
        spy.ops()
    );

    Ok(())
}

#[tokio::test]
async fn root_token_orchestration_reads_never_list_or_read_ledger() -> TestResult {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let spy = Arc::new(SpyBackend::new(inner));
    let storage = scoped_storage(spy.clone());
    let compactor = MicroCompactor::new(storage.clone());

    let pinned_result = append_and_compact(
        &storage,
        &compactor,
        run_triggered_event("run-root-pinned-01"),
    )
    .await?;
    let (pointer, _manifest) = current_orchestration_head(&storage).await?;
    seed_root_token_for_orchestration(
        &storage,
        "01JFLOWROOTTOKEN000000000001",
        &pointer,
        &pinned_result,
    )
    .await?;

    append_and_compact(
        &storage,
        &compactor,
        run_triggered_event("run-current-only-01"),
    )
    .await?;

    let (_current_manifest, current_state) = compactor.load_state().await?;
    assert!(current_state.runs.contains_key("run-root-pinned-01"));
    assert!(current_state.runs.contains_key("run-current-only-01"));

    spy.clear_ops();
    spy.set_fail_on_list(true);
    spy.add_fail_get_prefix(format!(
        "tenant={TENANT}/workspace={WORKSPACE}/ledger/orchestration/"
    ));

    let (_pinned_manifest, pinned_state) = compactor
        .load_state_for_root_token("root:01JFLOWROOTTOKEN000000000001")
        .await?;
    assert!(pinned_state.runs.contains_key("run-root-pinned-01"));
    assert!(!pinned_state.runs.contains_key("run-current-only-01"));
    assert!(
        spy.ops().iter().all(|op| !matches!(op, SpyOp::List { .. })),
        "root-token orchestration reads must not call list(): {:?}",
        spy.ops()
    );

    Ok(())
}
