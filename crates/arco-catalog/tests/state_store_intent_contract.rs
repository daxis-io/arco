//! Commit-outcome and asynchronous-intent wire-contract tests.

use std::sync::Arc;

use arco_catalog::{
    ArcoStateStore, ControlMvpStateStore, LayoutMaintenanceIntentV1, LayoutMaintenanceReason,
    ModelStateStore, ProjectionIntentV1, StateScope, TxnOptions,
};
use arco_core::{MemoryBackend, ScopedStorage};
use bytes::Bytes;

fn model_store() -> ModelStateStore {
    ModelStateStore::new(StateScope::new("tenant", "workspace", "metastore"))
}

#[tokio::test]
async fn transaction_commit_returns_state_token_and_intents_as_one_outcome() {
    let store = model_store();
    let mut txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalogs/sales", Bytes::from_static(b"active"))
        .await
        .expect("stage write");

    let outcome = txn.commit().await.expect("commit");

    assert_eq!(1, outcome.state_token().logical_sequence());
    assert!(outcome.projection_intents().is_empty());
}

#[tokio::test]
async fn projection_intent_v1_round_trips_with_opaque_source_authority() {
    let store = model_store();
    let token = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin transaction")
        .commit()
        .await
        .expect("commit")
        .into_state_token();
    let intent = ProjectionIntentV1::new(
        "project-0001",
        "system_catalog_tables",
        &token,
        Bytes::from_static(br#"{"table_id":"table-1"}"#),
    )
    .expect("projection intent");

    let encoded = serde_json::to_vec(&intent).expect("serialize intent");
    let decoded: ProjectionIntentV1 = serde_json::from_slice(&encoded).expect("decode intent");

    assert_eq!(
        ProjectionIntentV1::CONTRACT_VERSION,
        decoded.contract_version()
    );
    assert_eq!("project-0001", decoded.intent_id());
    assert_eq!("system_catalog_tables", decoded.projection_kind());
    assert_eq!(token.scope(), decoded.source_scope());
    assert_eq!(token.logical_sequence(), decoded.source_logical_sequence());
    assert_eq!(
        token.authority_manifest_id(),
        decoded.source_authority_manifest_id()
    );
    assert_eq!(br#"{"table_id":"table-1"}"#, decoded.payload());
}

#[tokio::test]
async fn layout_maintenance_intent_is_separate_from_logical_visibility() {
    let store = Arc::new(model_store());
    let token = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin transaction")
        .commit()
        .await
        .expect("commit")
        .into_state_token();
    let intent = LayoutMaintenanceIntentV1::new(
        "maintain-0001",
        &token,
        7,
        LayoutMaintenanceReason::L0SegmentCount,
    )
    .expect("maintenance intent");

    let encoded = serde_json::to_vec(&intent).expect("serialize intent");
    let decoded: LayoutMaintenanceIntentV1 =
        serde_json::from_slice(&encoded).expect("decode intent");

    assert_eq!(1, decoded.observed_logical_sequence());
    assert_eq!(7, decoded.layout_generation());
    assert_eq!(LayoutMaintenanceReason::L0SegmentCount, decoded.reason());
    assert_eq!(token.scope(), decoded.source_scope());
    assert_eq!(token.logical_sequence(), decoded.source_logical_sequence());
    assert_eq!(
        token.authority_manifest_id(),
        decoded.source_authority_manifest_id()
    );
}

#[tokio::test]
async fn control_commit_returns_and_durably_records_projection_intent() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage");
    let store =
        ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "metastore"))
            .expect("control store");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.stage_projection_intent(
        "project-0001",
        "system_catalog_tables",
        Bytes::from_static(br#"{"table_id":"table-1"}"#),
    )
    .expect("stage projection intent");

    let outcome = txn.commit().await.expect("commit");

    assert_eq!(1, outcome.projection_intents().len());
    let committed = &outcome.projection_intents()[0];
    assert_eq!(outcome.state_token().scope(), committed.source_scope());
    assert_eq!(
        outcome.state_token().logical_sequence(),
        committed.source_logical_sequence()
    );
    assert_eq!(
        outcome.state_token().authority_manifest_id(),
        committed.source_authority_manifest_id()
    );
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("read committed outbox");
    assert_eq!(1, outbox.len());
    let durable: ProjectionIntentV1 =
        serde_json::from_slice(outbox[0].payload()).expect("decode durable intent");
    assert_eq!(committed, &durable);
}
