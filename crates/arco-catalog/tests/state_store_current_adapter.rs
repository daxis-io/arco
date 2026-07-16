//! Current state-store adapter contract tests.

use std::sync::Arc;

use bytes::Bytes;
use chrono::{Duration, Utc};

use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateStore, ArcoStateTxn, CatalogError, CheckpointOptions,
    ControlMvpStateStore, CurrentStateStore, PersistedAuthorityAdapter, StateScope, TxnOptions,
};
use arco_core::{MemoryBackend, ScopedStorage};

fn assert_reader_surface(_: &dyn ArcoStateReader) {}

fn assert_admin_surface(_: &dyn ArcoStateAdmin) {}

fn assert_store_surface(_: &dyn ArcoStateStore) {}

fn assert_unsupported<T>(result: arco_catalog::Result<T>, expected: &str) {
    match result {
        Err(CatalogError::UnsupportedOperation { .. }) => {}
        Err(error) => panic!("expected UnsupportedOperation for {expected}, got {error:?}"),
        Ok(_) => panic!("expected UnsupportedOperation for {expected}"),
    }
}

#[test]
fn current_state_store_implements_trait_object_surface() {
    let store = CurrentStateStore::new();

    assert_reader_surface(&store);
    assert_admin_surface(&store);
    assert_store_surface(&store);
}

#[test]
fn current_state_store_capabilities_are_explicitly_unsupported() {
    let capabilities = CurrentStateStore::new().capabilities();

    assert_eq!(capabilities.implementation(), "arco-state-current");
    assert!(!capabilities.retained_state_tokens());
    assert!(!capabilities.checkpoints());
    assert!(!capabilities.read_at());
    assert!(!capabilities.transactions());
    assert!(!capabilities.range_preconditions());
    assert!(!capabilities.predicate_preconditions());
}

#[tokio::test]
async fn current_state_store_rejects_capability_only_reader_operations() {
    let store = CurrentStateStore::new();

    assert_unsupported(store.get(b"catalog/default").await, "get");
    assert_unsupported(store.scan_prefix(b"catalog/").await, "scan_prefix");
}

#[tokio::test]
async fn current_state_store_rejects_future_token_checkpoint_and_transaction_operations() {
    let store = CurrentStateStore::new();

    assert_unsupported(store.current_state_token().await, "current_state_token");
    assert_unsupported(
        store.checkpoint(CheckpointOptions::default()).await,
        "checkpoint",
    );
    assert_unsupported(store.begin_txn(TxnOptions::default()).await, "begin_txn");
}

#[tokio::test]
async fn current_state_store_rejects_persisted_authority_adapter_operations() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage");
    let control =
        ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", "catalog"))
            .expect("control MVP store");
    let mut txn = control
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage value");
    let token = txn.commit().await.expect("commit");
    let checkpoint = control
        .checkpoint(CheckpointOptions::new(Some(StateScope::new(
            "tenant",
            "workspace",
            "catalog",
        ))))
        .await
        .expect("checkpoint");
    let deadline = Utc::now() + Duration::hours(1);
    let reference = control
        .persist_state_reference(&token, deadline)
        .await
        .expect("control reference");

    let current = CurrentStateStore::new();
    assert_unsupported(
        current.persist_state_reference(&token, deadline).await,
        "persist_state_reference",
    );
    assert_unsupported(
        current
            .persist_checkpoint_reference(&checkpoint, deadline)
            .await,
        "persist_checkpoint_reference",
    );
    assert_unsupported(
        current.resolve_persisted_reference(&reference).await,
        "resolve_persisted_reference",
    );
}
