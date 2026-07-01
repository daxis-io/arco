//! Current state-store adapter contract tests.

use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateStore, CatalogError, CheckpointOptions,
    CurrentStateStore, TxnOptions,
};

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
