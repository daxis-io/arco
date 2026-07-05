//! Deterministic state-store model contract tests.

use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateStore, CatalogError, KeyRange, ModelStateStore,
    PredicateInputSet, StateScope, TxnOptions,
};
use bytes::Bytes;

fn scope() -> StateScope {
    StateScope::new("tenant", "workspace", "catalog")
}

#[tokio::test]
async fn accepted_commits_advance_logical_sequence_once() {
    let store = ModelStateStore::new(scope());

    let mut first_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin first transaction");
    first_txn
        .put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage first write");
    let staged_pairs = first_txn
        .scan_prefix(b"catalog/")
        .await
        .expect("scan staged writes");
    assert_eq!(1, staged_pairs.len());
    assert_eq!(b"catalog/default", staged_pairs[0].key());
    assert_eq!(Bytes::from_static(b"v1"), *staged_pairs[0].value().bytes());
    assert_eq!(None, staged_pairs[0].value().generation());
    let first_token = first_txn.commit().await.expect("commit first transaction");

    assert_eq!(1, first_token.logical_sequence());
    assert_eq!(
        1,
        store
            .current_state_token()
            .await
            .expect("current state token")
            .logical_sequence()
    );

    let mut second_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin second transaction");
    second_txn
        .put(b"catalog/default", Bytes::from_static(b"v2"))
        .await
        .expect("stage second write");
    let second_token = second_txn
        .commit()
        .await
        .expect("commit second transaction");

    assert_eq!(2, second_token.logical_sequence());
    let first_reader = store
        .read_at(first_token)
        .await
        .expect("open retained first-sequence reader");
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        first_reader
            .get(b"catalog/default")
            .await
            .expect("read retained first value")
    );
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        store
            .get(b"catalog/default")
            .await
            .expect("read current value")
    );
    assert_eq!(2, store.committed_records().len());
    assert_eq!(
        vec![
            "sequence=1 request_id=<none> writes=put(catalog/default@1)",
            "sequence=2 request_id=<none> writes=put(catalog/default@2)",
        ],
        store.explain_transitions()
    );
}

#[tokio::test]
async fn transaction_scope_mismatch_fails_closed() {
    let store = ModelStateStore::new(scope());
    let mismatched_scope = StateScope::new("tenant", "other-workspace", "catalog");

    let result = store
        .begin_txn(TxnOptions::new(Some(mismatched_scope)))
        .await;

    assert!(matches!(result, Err(CatalogError::Validation { .. })));
    assert_eq!(
        0,
        store
            .current_state_token()
            .await
            .expect("current state token")
            .logical_sequence()
    );
    assert!(store.committed_records().is_empty());
}

#[tokio::test]
async fn failed_precondition_revalidation_does_not_advance_sequence() {
    let store = ModelStateStore::new(scope());

    let mut stale_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin stale transaction");
    stale_txn
        .assert_absent(b"catalog/default")
        .await
        .expect("record absent precondition");

    let mut winning_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin winning transaction");
    winning_txn
        .put(b"catalog/default", Bytes::from_static(b"winner"))
        .await
        .expect("stage winning write");
    let winning_token = winning_txn
        .commit()
        .await
        .expect("commit winning transaction");
    assert_eq!(1, winning_token.logical_sequence());

    stale_txn
        .put(b"catalog/default", Bytes::from_static(b"stale"))
        .await
        .expect("stage stale write");
    let stale_error = stale_txn
        .commit()
        .await
        .expect_err("stale transaction should fail closed");

    assert!(matches!(
        stale_error,
        CatalogError::PreconditionFailed { .. }
    ));
    assert_eq!(
        1,
        store
            .current_state_token()
            .await
            .expect("current state token")
            .logical_sequence()
    );
    assert_eq!(1, store.committed_records().len());
}

#[tokio::test]
async fn point_precondition_failure_fails_closed() {
    let store = ModelStateStore::new(scope());

    let mut seed_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin seed transaction");
    seed_txn
        .put(b"catalog/default", Bytes::from_static(b"seed"))
        .await
        .expect("stage seed write");
    seed_txn.commit().await.expect("commit seed transaction");

    let mut stale_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin stale transaction");
    stale_txn
        .assert_generation(b"catalog/default", 1)
        .await
        .expect("record generation precondition");

    let mut winning_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin winning transaction");
    winning_txn
        .put(b"catalog/default", Bytes::from_static(b"winner"))
        .await
        .expect("stage winning write");
    winning_txn
        .commit()
        .await
        .expect("commit winning transaction");

    stale_txn
        .put(b"catalog/default", Bytes::from_static(b"stale"))
        .await
        .expect("stage stale write");
    let stale_error = stale_txn
        .commit()
        .await
        .expect_err("stale generation should fail closed");

    assert!(matches!(
        stale_error,
        CatalogError::PreconditionFailed { .. }
    ));
    assert_eq!(
        Some(Bytes::from_static(b"winner")),
        store.get(b"catalog/default").await.expect("read winner")
    );
    assert_eq!(
        2,
        store
            .current_state_token()
            .await
            .expect("current state token")
            .logical_sequence()
    );
    assert_eq!(2, store.committed_records().len());
}

#[tokio::test]
async fn range_empty_precondition_failure_fails_closed() {
    let store = ModelStateStore::new(scope());
    let range = KeyRange::new(b"catalog/".to_vec(), b"catalog0".to_vec());

    let mut stale_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin stale transaction");
    stale_txn
        .assert_range_empty(range)
        .await
        .expect("record empty range precondition");

    let mut winning_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin winning transaction");
    winning_txn
        .put(b"catalog/default", Bytes::from_static(b"winner"))
        .await
        .expect("stage winning write");
    winning_txn
        .commit()
        .await
        .expect("commit winning transaction");

    stale_txn
        .put(b"catalog/other", Bytes::from_static(b"stale"))
        .await
        .expect("stage stale write");
    let stale_error = stale_txn
        .commit()
        .await
        .expect_err("stale range-empty assertion should fail closed");

    assert!(matches!(
        stale_error,
        CatalogError::PreconditionFailed { .. }
    ));
    assert_eq!(1, store.committed_records().len());
    assert_eq!(
        Some(Bytes::from_static(b"winner")),
        store.get(b"catalog/default").await.expect("read winner")
    );
    assert_eq!(None, store.get(b"catalog/other").await.expect("read stale"));
}

#[tokio::test]
async fn range_unchanged_precondition_failure_fails_closed() {
    let store = ModelStateStore::new(scope());
    let range = KeyRange::new(b"catalog/".to_vec(), b"catalog0".to_vec());

    let mut seed_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin seed transaction");
    seed_txn
        .put(b"catalog/default", Bytes::from_static(b"seed"))
        .await
        .expect("stage seed write");
    seed_txn.commit().await.expect("commit seed transaction");

    let observed = store.range_witness(&range);
    let mut stale_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin stale transaction");
    stale_txn
        .assert_range_unchanged(range, observed)
        .await
        .expect("record unchanged range precondition");

    let mut winning_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin winning transaction");
    winning_txn
        .put(b"catalog/default", Bytes::from_static(b"winner"))
        .await
        .expect("stage winning write");
    winning_txn
        .commit()
        .await
        .expect("commit winning transaction");

    stale_txn
        .put(b"catalog/other", Bytes::from_static(b"stale"))
        .await
        .expect("stage stale write");
    let stale_error = stale_txn
        .commit()
        .await
        .expect_err("stale range witness should fail closed");

    assert!(matches!(
        stale_error,
        CatalogError::PreconditionFailed { .. }
    ));
    assert_eq!(
        Some(Bytes::from_static(b"winner")),
        store.get(b"catalog/default").await.expect("read winner")
    );
    assert_eq!(None, store.get(b"catalog/other").await.expect("read stale"));
    assert_eq!(2, store.committed_records().len());
}

#[tokio::test]
async fn predicate_input_set_revalidation_catches_conflicting_writes() {
    let store = ModelStateStore::new(scope());
    let point_inputs = vec![b"catalog/default".to_vec()];
    let range_inputs = vec![KeyRange::new(b"catalog/".to_vec(), b"catalog0".to_vec())];

    let mut stale_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin stale transaction");
    let inputs = stale_txn
        .read_set(&point_inputs, &range_inputs)
        .await
        .expect("record predicate read set");
    assert_eq!(
        PredicateInputSet::new(point_inputs.clone(), range_inputs.clone()),
        inputs
    );
    stale_txn
        .assert_inputs_unchanged(inputs)
        .await
        .expect("record predicate precondition");

    let mut winning_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin winning transaction");
    winning_txn
        .put(b"catalog/default", Bytes::from_static(b"winner"))
        .await
        .expect("stage winning write");
    winning_txn
        .commit()
        .await
        .expect("commit winning transaction");

    stale_txn
        .put(b"catalog/result", Bytes::from_static(b"stale"))
        .await
        .expect("stage stale write");
    let stale_error = stale_txn
        .commit()
        .await
        .expect_err("stale predicate inputs should fail closed");

    assert!(matches!(
        stale_error,
        CatalogError::PreconditionFailed { .. }
    ));
    assert_eq!(
        Some(Bytes::from_static(b"winner")),
        store.get(b"catalog/default").await.expect("read winner")
    );
    assert_eq!(
        None,
        store.get(b"catalog/result").await.expect("read stale")
    );
    assert_eq!(1, store.committed_records().len());
}

#[tokio::test]
async fn replay_from_committed_events_equals_folded_kv_state() {
    let store = ModelStateStore::new(scope());

    let mut first_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin first transaction");
    first_txn
        .put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage first write");
    first_txn
        .put(b"catalog/other", Bytes::from_static(b"v2"))
        .await
        .expect("stage second write");
    first_txn.commit().await.expect("commit first transaction");

    let mut second_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin second transaction");
    second_txn
        .delete(b"catalog/default")
        .await
        .expect("stage delete");
    second_txn
        .commit()
        .await
        .expect("commit second transaction");

    let replayed =
        ModelStateStore::replay_from_committed_records(scope(), store.committed_records())
            .expect("replay committed records");

    assert_eq!(store.folded_entries(), replayed.folded_entries());
    assert_eq!(store.explain_transitions(), replayed.explain_transitions());
    assert_eq!(
        vec![
            (b"catalog/default".to_vec(), None, 2,),
            (
                b"catalog/other".to_vec(),
                Some(Bytes::from_static(b"v2")),
                1,
            ),
        ],
        replayed.folded_entries()
    );
}

#[tokio::test]
async fn idempotent_replay_is_stable() {
    let store = ModelStateStore::new(scope());

    let mut txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage write");
    txn.commit().await.expect("commit transaction");

    let records = store.committed_records();
    let duplicated_records = records
        .iter()
        .cloned()
        .chain(records.iter().cloned())
        .collect::<Vec<_>>();

    let replayed = ModelStateStore::replay_from_committed_records(scope(), duplicated_records)
        .expect("replay duplicated records");

    assert_eq!(records, replayed.committed_records());
    assert_eq!(store.folded_entries(), replayed.folded_entries());
    assert_eq!(
        ["put catalog/default generation=1 bytes=2"],
        replayed.committed_records()[0].logical_events()
    );
}

#[tokio::test]
async fn failed_transactions_publish_no_partial_state() {
    let store = ModelStateStore::new(scope());

    let mut seed_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin seed transaction");
    seed_txn
        .put(b"catalog/existing", Bytes::from_static(b"seed"))
        .await
        .expect("stage seed write");
    seed_txn.commit().await.expect("commit seed transaction");

    let mut stale_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin stale transaction");
    stale_txn
        .assert_generation(b"catalog/existing", 1)
        .await
        .expect("record generation precondition");
    stale_txn
        .delete(b"catalog/existing")
        .await
        .expect("stage stale delete");
    stale_txn
        .put(b"catalog/new", Bytes::from_static(b"stale"))
        .await
        .expect("stage stale put");

    let mut winning_txn = store
        .begin_txn(TxnOptions::default())
        .await
        .expect("begin winning transaction");
    winning_txn
        .put(b"catalog/existing", Bytes::from_static(b"winner"))
        .await
        .expect("stage winning write");
    winning_txn
        .commit()
        .await
        .expect("commit winning transaction");

    let stale_error = stale_txn
        .commit()
        .await
        .expect_err("failed transaction should not publish partial state");

    assert!(matches!(
        stale_error,
        CatalogError::PreconditionFailed { .. }
    ));
    assert_eq!(
        Some(Bytes::from_static(b"winner")),
        store.get(b"catalog/existing").await.expect("read winner")
    );
    assert_eq!(None, store.get(b"catalog/new").await.expect("read new key"));
    assert_eq!(
        2,
        store
            .current_state_token()
            .await
            .expect("current state token")
            .logical_sequence()
    );
    assert_eq!(2, store.committed_records().len());
}
