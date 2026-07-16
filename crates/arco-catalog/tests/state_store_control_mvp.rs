//! Object-store control-state MVP contract tests.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, CatalogError, CheckpointOptions,
    ControlMvpPaths, ControlMvpProjectionOutboxRecord, ControlMvpStateStore, KeyRange,
    PersistedAuthorityAdapter, PersistedAuthorityKind, PersistedAuthorityReference, StateScope,
    TxnOptions,
};
use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ScopedStorage};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;

use chrono::{Duration as ChronoDuration, Utc};

fn scope() -> StateScope {
    StateScope::new("tenant", "workspace", "catalog")
}

fn storage() -> (Arc<MemoryBackend>, ScopedStorage) {
    let backend = Arc::new(MemoryBackend::new());
    let storage =
        ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
    (backend, storage)
}

fn store(storage: ScopedStorage) -> ControlMvpStateStore {
    ControlMvpStateStore::new(storage, scope()).expect("control MVP store")
}

#[tokio::test]
async fn persisted_authority_references_round_trip_state_tokens_and_checkpoints() {
    let (_backend, storage) = storage();
    let store = store(storage);
    let paths = ControlMvpPaths::new("catalog");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage value");
    let token = txn.commit().await.expect("commit");
    let checkpoint = store
        .checkpoint(CheckpointOptions::new(Some(scope())).with_min_retention_seconds(60))
        .await
        .expect("checkpoint");
    let deadline = Utc::now() + ChronoDuration::hours(1);

    let state_reference = store
        .persist_state_reference(&token, deadline)
        .await
        .expect("persist state reference");
    assert_eq!("arco-state-control-mvp", state_reference.implementation());
    assert_eq!(&scope(), state_reference.scope());
    assert_eq!(
        PersistedAuthorityKind::StateToken,
        state_reference.reference_kind()
    );
    assert_eq!(token.authority_manifest_id(), state_reference.manifest_id());
    assert_eq!(token.logical_sequence(), state_reference.logical_sequence());
    assert_eq!(
        paths.manifest_object(token.authority_manifest_id()),
        state_reference.manifest_path()
    );
    assert!(state_reference.manifest_sha256().starts_with("sha256:"));
    assert_eq!(None, state_reference.checkpoint_path());
    assert_eq!(deadline, state_reference.retention_deadline());

    let checkpoint_reference = store
        .persist_checkpoint_reference(&checkpoint, deadline)
        .await
        .expect("persist checkpoint reference");
    assert_eq!(
        PersistedAuthorityKind::Checkpoint,
        checkpoint_reference.reference_kind()
    );
    assert_eq!(
        Some(paths.checkpoint_object(checkpoint.checkpoint_id()).as_str()),
        checkpoint_reference.checkpoint_path()
    );
    assert!(
        checkpoint_reference
            .checkpoint_sha256()
            .expect("checkpoint digest")
            .starts_with("sha256:")
    );

    for reference in [&state_reference, &checkpoint_reference] {
        let reader = store
            .resolve_persisted_reference(reference)
            .await
            .expect("resolve reference");
        assert_eq!(
            Some(Bytes::from_static(b"v1")),
            reader.get(b"catalog/default").await.expect("retained read")
        );
        assert!(
            store
                .resolve_persisted_reference_at(reference, deadline + ChronoDuration::seconds(1),)
                .await
                .is_err(),
            "caller-supplied time after the deadline must expire the reference"
        );
        let json = serde_json::to_string(reference).expect("reference json");
        assert!(!json.contains("StateToken"));
        assert!(!json.contains("CheckpointToken"));
    }
}

#[tokio::test]
async fn persisted_authority_resolution_revalidates_every_stable_field() {
    let (_backend, storage) = storage();
    let store = store(storage);
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage value");
    let token = txn.commit().await.expect("commit");
    let reference = store
        .persist_state_reference(&token, Utc::now() + ChronoDuration::hours(1))
        .await
        .expect("persist reference");

    let mutations = [
        ("implementation", Value::String("other-backend".to_string())),
        ("manifest_id", Value::String("other-manifest".to_string())),
        ("logical_sequence", Value::from(99_u64)),
        (
            "manifest_path",
            Value::String("other/path.json".to_string()),
        ),
        (
            "manifest_sha256",
            Value::String(format!("sha256:{}", "f".repeat(64))),
        ),
        (
            "retention_deadline",
            Value::String("2000-01-01T00:00:00Z".to_string()),
        ),
    ];
    for (field, replacement) in mutations {
        let mut value = serde_json::to_value(&reference).expect("reference json");
        value[field] = replacement;
        let corrupt: PersistedAuthorityReference =
            serde_json::from_value(value).expect("reference shape");
        assert!(
            store.resolve_persisted_reference(&corrupt).await.is_err(),
            "corrupt {field} must fail closed"
        );
    }

    let mut value = serde_json::to_value(&reference).expect("reference json");
    value["scope"]["workspace_id"] = Value::String("other-workspace".to_string());
    let corrupt_scope: PersistedAuthorityReference =
        serde_json::from_value(value).expect("reference shape");
    assert!(
        store
            .resolve_persisted_reference(&corrupt_scope)
            .await
            .is_err()
    );

    let incoherent = PersistedAuthorityReference::new(
        "arco-state-control-mvp",
        scope(),
        PersistedAuthorityKind::StateToken,
        token.authority_manifest_id(),
        token.logical_sequence(),
        ControlMvpPaths::new("catalog").manifest_object(token.authority_manifest_id()),
        format!("sha256:{}", "1".repeat(64)),
        Some("checkpoints/not-allowed.json".to_string()),
        Some(format!("sha256:{}", "2".repeat(64))),
        Utc::now() + ChronoDuration::hours(1),
    );
    assert!(incoherent.is_err());
}

#[tokio::test]
async fn checkpoint_references_reject_drive_paths_digest_corruption_and_missing_fields() {
    let (_backend, storage) = storage();
    let store = store(storage);
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage value");
    txn.commit().await.expect("commit");
    let checkpoint = store
        .checkpoint(CheckpointOptions::new(Some(scope())))
        .await
        .expect("checkpoint");
    let reference = store
        .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::hours(1))
        .await
        .expect("checkpoint reference");

    let mut drive_path = serde_json::to_value(&reference).expect("reference json");
    drive_path["checkpoint_path"] = Value::String("C:/outside/checkpoint.json".to_string());
    let drive_path: PersistedAuthorityReference =
        serde_json::from_value(drive_path).expect("reference shape");
    assert!(
        drive_path.validate().is_err(),
        "persisted authority validation must reject drive-qualified absolute paths"
    );

    let mutations = [
        (
            "checkpoint_path",
            Value::String("state-store/control-mvp/catalog/checkpoints/other.json".to_string()),
        ),
        (
            "checkpoint_sha256",
            Value::String(format!("sha256:{}", "f".repeat(64))),
        ),
        ("checkpoint_path", Value::Null),
        ("checkpoint_sha256", Value::Null),
    ];
    for (field, replacement) in mutations {
        let mut value = serde_json::to_value(&reference).expect("reference json");
        value[field] = replacement;
        let corrupt: PersistedAuthorityReference =
            serde_json::from_value(value).expect("reference shape");
        assert!(
            corrupt.validate().is_err()
                || store.resolve_persisted_reference(&corrupt).await.is_err(),
            "corrupt or missing {field} must fail closed"
        );
    }
}

#[tokio::test]
async fn committed_tx_writes_immutable_artifacts_then_cas_publishes_pointer() {
    let (_backend, storage) = storage();
    let store = store(storage.clone());
    let paths = ControlMvpPaths::new("catalog");

    let mut txn = store
        .begin_control_txn(TxnOptions::default().with_request_id("seed"))
        .await
        .expect("begin control transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage write");
    let tx_id = txn.tx_id().to_string();
    let manifest_id = txn.candidate_manifest_id().to_string();

    let token = txn.commit().await.expect("commit transaction");

    assert_eq!(
        "arco-state-control-mvp",
        store.capabilities().implementation()
    );
    assert!(store.capabilities().retained_state_tokens());
    assert!(store.capabilities().checkpoints());
    assert!(store.capabilities().read_at());
    assert!(store.capabilities().transactions());
    assert_eq!(1, token.logical_sequence());
    assert_eq!(manifest_id, token.authority_manifest_id());
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        store.get(b"catalog/default").await.expect("current read")
    );

    storage
        .get_raw(&paths.tx_object(&tx_id))
        .await
        .expect("immutable tx object exists");
    storage
        .get_raw(&paths.manifest_object(&manifest_id))
        .await
        .expect("immutable manifest object exists");
    let pointer = storage
        .get_raw(&paths.current_pointer())
        .await
        .expect("published pointer exists");
    let pointer_json: Value = serde_json::from_slice(&pointer).expect("pointer json");
    assert_eq!(manifest_id, pointer_json["manifest_id"]);

    let duplicate_tx = storage
        .put_raw(
            &paths.tx_object(&tx_id),
            Bytes::from_static(b"duplicate"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("duplicate tx write returns precondition result");
    assert!(matches!(
        duplicate_tx,
        WriteResult::PreconditionFailed { .. }
    ));

    let duplicate_manifest = storage
        .put_raw(
            &paths.manifest_object(&manifest_id),
            Bytes::from_static(b"duplicate"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("duplicate manifest write returns precondition result");
    assert!(matches!(
        duplicate_manifest,
        WriteResult::PreconditionFailed { .. }
    ));
}

#[tokio::test]
async fn successful_commit_returns_one_visible_state_token() {
    let (_backend, storage) = storage();
    let store = store(storage);

    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage write");
    let token = txn.commit().await.expect("commit");

    assert_eq!(1, token.logical_sequence());
    assert_eq!(
        token,
        store
            .current_state_token()
            .await
            .expect("current state token")
    );
}

#[tokio::test]
async fn cas_loss_leaves_old_state_and_old_outbox_visible_only() {
    let (_backend, storage) = storage();
    let store = store(storage.clone());
    let paths = ControlMvpPaths::new("catalog");

    let mut stale_txn = store
        .begin_control_txn(TxnOptions::default().with_request_id("stale"))
        .await
        .expect("begin stale transaction");
    stale_txn
        .put(b"catalog/default", Bytes::from_static(b"stale"))
        .await
        .expect("stage stale write");
    stale_txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "stale-outbox",
        Bytes::from_static(b"stale"),
    ));
    let stale_tx_id = stale_txn.tx_id().to_string();
    let stale_manifest_id = stale_txn.candidate_manifest_id().to_string();

    let mut winning_txn = store
        .begin_control_txn(TxnOptions::default().with_request_id("winning"))
        .await
        .expect("begin winning transaction");
    winning_txn
        .put(b"catalog/default", Bytes::from_static(b"winner"))
        .await
        .expect("stage winning write");
    winning_txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "winning-outbox",
        Bytes::from_static(b"winner"),
    ));
    let winning_token = winning_txn.commit().await.expect("commit winner");

    let stale_error = stale_txn
        .commit()
        .await
        .expect_err("stale pointer CAS must fail");

    assert!(matches!(stale_error, CatalogError::CasFailed { .. }));
    assert_eq!(1, winning_token.logical_sequence());
    assert_eq!(
        Some(Bytes::from_static(b"winner")),
        store.get(b"catalog/default").await.expect("current read")
    );
    assert_eq!(
        vec![ControlMvpProjectionOutboxRecord::new(
            "winning-outbox",
            Bytes::from_static(b"winner"),
        )],
        store
            .current_projection_outbox()
            .await
            .expect("current outbox")
    );
    storage
        .get_raw(&paths.tx_object(&stale_tx_id))
        .await
        .expect("losing tx artifact remains physical");
    storage
        .get_raw(&paths.manifest_object(&stale_manifest_id))
        .await
        .expect("losing manifest artifact remains physical");
    assert_ne!(
        stale_manifest_id,
        store
            .current_state_token()
            .await
            .expect("current token")
            .authority_manifest_id()
    );
}

#[tokio::test]
async fn unreachable_manifest_artifacts_are_invisible_without_pointer_reachability() {
    let (_backend, storage) = storage();
    let store = store(storage);

    let mut stale_txn = store
        .begin_control_txn(TxnOptions::default().with_request_id("stale"))
        .await
        .expect("begin stale transaction");
    stale_txn
        .put(b"catalog/hidden", Bytes::from_static(b"stale"))
        .await
        .expect("stage stale write");
    stale_txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "hidden-outbox",
        Bytes::from_static(b"hidden"),
    ));

    let mut winning_txn = store
        .begin_control_txn(TxnOptions::default().with_request_id("winning"))
        .await
        .expect("begin winning transaction");
    winning_txn
        .put(b"catalog/visible", Bytes::from_static(b"winner"))
        .await
        .expect("stage winning write");
    let token = winning_txn.commit().await.expect("commit winner");

    assert!(matches!(
        stale_txn.commit().await,
        Err(CatalogError::CasFailed { .. })
    ));

    let retained = store.read_at(token).await.expect("read retained manifest");
    assert_eq!(
        Some(Bytes::from_static(b"winner")),
        retained
            .get(b"catalog/visible")
            .await
            .expect("retained visible read")
    );
    assert_eq!(
        None,
        retained
            .get(b"catalog/hidden")
            .await
            .expect("retained hidden read")
    );
    assert!(
        store
            .current_projection_outbox()
            .await
            .expect("current outbox")
            .is_empty()
    );
}

#[tokio::test]
async fn read_at_state_token_resolves_retained_manifest_state() {
    let (_backend, storage) = storage();
    let store = store(storage);

    let mut first_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin first transaction");
    first_txn
        .put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage first write");
    let first_token = first_txn.commit().await.expect("commit first transaction");

    let mut second_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin second transaction");
    second_txn
        .put(b"catalog/default", Bytes::from_static(b"v2"))
        .await
        .expect("stage second write");
    second_txn
        .commit()
        .await
        .expect("commit second transaction");

    let first_reader = store
        .read_at(first_token)
        .await
        .expect("open first retained reader");

    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        first_reader
            .get(b"catalog/default")
            .await
            .expect("read first value")
    );
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        store.get(b"catalog/default").await.expect("read current")
    );
}

#[tokio::test]
async fn manifest_reachable_replay_folds_expected_kv_state() {
    let (_backend, storage) = storage();
    let store = store(storage);

    let mut first_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin first transaction");
    first_txn
        .put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage default");
    first_txn
        .put(b"catalog/other", Bytes::from_static(b"v2"))
        .await
        .expect("stage other");
    first_txn.commit().await.expect("commit first");

    let mut second_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin second transaction");
    second_txn
        .delete(b"catalog/default")
        .await
        .expect("stage delete");
    let token = second_txn.commit().await.expect("commit second");

    let reader = store.read_at(token).await.expect("read retained state");
    assert_eq!(
        None,
        reader
            .get(b"catalog/default")
            .await
            .expect("deleted key absent")
    );
    assert_eq!(
        vec![(
            b"catalog/other".to_vec(),
            Bytes::from_static(b"v2"),
            Some(1)
        )],
        reader
            .scan_prefix(b"catalog/")
            .await
            .expect("scan folded state")
            .into_iter()
            .map(|pair| {
                (
                    pair.key().to_vec(),
                    pair.value().bytes().clone(),
                    pair.value().generation(),
                )
            })
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn projection_outbox_records_are_visible_only_after_manifest_is_visible() {
    let (_backend, storage) = storage();
    let store = store(storage);

    let mut first_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin first transaction");
    first_txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "first",
        Bytes::from_static(b"payload-1"),
    ));
    let first_token = first_txn.commit().await.expect("commit first");

    let mut stale_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin stale transaction");
    stale_txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "stale",
        Bytes::from_static(b"payload-stale"),
    ));

    let mut winning_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin winning transaction");
    winning_txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "second",
        Bytes::from_static(b"payload-2"),
    ));
    let second_token = winning_txn.commit().await.expect("commit second");
    assert!(matches!(
        stale_txn.commit().await,
        Err(CatalogError::CasFailed { .. })
    ));

    assert_eq!(
        vec![ControlMvpProjectionOutboxRecord::new(
            "first",
            Bytes::from_static(b"payload-1"),
        )],
        store
            .projection_outbox_at(first_token)
            .await
            .expect("first outbox")
    );
    assert_eq!(
        vec![
            ControlMvpProjectionOutboxRecord::new("first", Bytes::from_static(b"payload-1")),
            ControlMvpProjectionOutboxRecord::new("second", Bytes::from_static(b"payload-2")),
        ],
        store
            .projection_outbox_at(second_token)
            .await
            .expect("second outbox")
    );
    assert_eq!(
        vec![
            ControlMvpProjectionOutboxRecord::new("first", Bytes::from_static(b"payload-1")),
            ControlMvpProjectionOutboxRecord::new("second", Bytes::from_static(b"payload-2")),
        ],
        store
            .current_projection_outbox()
            .await
            .expect("current outbox")
    );
}

#[tokio::test]
async fn checksum_or_corrupt_artifact_failure_fails_closed() {
    let (_backend, storage) = storage();
    let store = store(storage.clone());
    let paths = ControlMvpPaths::new("catalog");

    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage write");
    let token = txn.commit().await.expect("commit");

    let manifest_path = paths.manifest_object(token.authority_manifest_id());
    let original = storage
        .get_raw(&manifest_path)
        .await
        .expect("manifest object");
    let mut manifest_json: Value = serde_json::from_slice(&original).expect("manifest json");
    manifest_json["checksum_sha256"] = Value::String("deadbeef".to_string());
    storage
        .put_raw(
            &manifest_path,
            Bytes::from(serde_json::to_vec(&manifest_json).expect("manifest bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("corrupt manifest checksum");

    let error = match store.read_at(token).await {
        Err(error) => error,
        Ok(_) => panic!("checksum mismatch must fail closed"),
    };
    assert!(matches!(
        error,
        CatalogError::InvariantViolation { .. } | CatalogError::Serialization { .. }
    ));
}

#[tokio::test]
async fn pointer_manifest_checksum_mismatch_fails_closed() {
    let (_backend, storage) = storage();
    let store = store(storage.clone());
    let paths = ControlMvpPaths::new("catalog");

    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage write");
    let token = txn.commit().await.expect("commit");

    rewrite_object_pretty(
        &storage,
        &paths.manifest_object(token.authority_manifest_id()),
    )
    .await;

    let error = store
        .get(b"catalog/default")
        .await
        .expect_err("pointer manifest checksum mismatch must fail closed");
    assert!(matches!(error, CatalogError::InvariantViolation { .. }));
}

#[tokio::test]
async fn manifest_transaction_checksum_mismatch_fails_closed() {
    let (_backend, storage) = storage();
    let store = store(storage.clone());
    let paths = ControlMvpPaths::new("catalog");

    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage write");
    let tx_id = txn.tx_id().to_string();
    let token = txn.commit().await.expect("commit");

    rewrite_object_pretty(&storage, &paths.tx_object(&tx_id)).await;

    let error = match store.read_at(token).await {
        Err(error) => error,
        Ok(_) => panic!("manifest transaction checksum mismatch must fail closed"),
    };
    assert!(matches!(error, CatalogError::InvariantViolation { .. }));
}

#[tokio::test]
async fn tombstoned_keys_keep_range_empty_preconditions_from_succeeding() {
    let (_backend, storage) = storage();
    let store = store(storage);
    let range = KeyRange::new(b"catalog/".to_vec(), b"catalog0".to_vec());

    let mut seed_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin seed transaction");
    seed_txn
        .put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage seed");
    seed_txn.commit().await.expect("commit seed");

    let mut delete_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin delete transaction");
    delete_txn
        .delete(b"catalog/default")
        .await
        .expect("stage delete");
    delete_txn.commit().await.expect("commit delete");

    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin range assertion transaction");
    let error = txn
        .assert_range_empty(range)
        .await
        .expect_err("tombstoned key should still occupy the folded range");
    assert!(matches!(error, CatalogError::PreconditionFailed { .. }));
}

#[tokio::test]
async fn checkpoint_reads_open_the_retained_manifest_reader() {
    let (_backend, storage) = storage();
    let store = store(storage);

    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage write");
    txn.commit().await.expect("commit");
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .expect("checkpoint current manifest");

    let mut second_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin second transaction");
    second_txn
        .put(b"catalog/default", Bytes::from_static(b"v2"))
        .await
        .expect("stage second write");
    second_txn.commit().await.expect("commit second");

    let checkpoint_reader = store
        .read_checkpoint(checkpoint)
        .await
        .expect("open checkpoint reader");
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        checkpoint_reader
            .get(b"catalog/default")
            .await
            .expect("checkpoint retained value")
    );
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        store.get(b"catalog/default").await.expect("current value")
    );
}

#[tokio::test]
async fn request_time_correctness_paths_do_not_call_object_store_listing() {
    let backend = Arc::new(NoListBackend::new(Arc::new(MemoryBackend::new())));
    let storage =
        ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
    let store = store(storage);

    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage write");
    let token = txn.commit().await.expect("commit");
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        store.get(b"catalog/default").await.expect("current read")
    );
    let retained = store.read_at(token).await.expect("retained reader");
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        retained
            .get(b"catalog/default")
            .await
            .expect("retained read")
    );
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .expect("checkpoint");
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        store
            .read_checkpoint(checkpoint)
            .await
            .expect("checkpoint reader")
            .get(b"catalog/default")
            .await
            .expect("checkpoint read")
    );

    assert_eq!(0, backend.list_calls());
}

struct NoListBackend {
    inner: Arc<dyn StorageBackend>,
    list_calls: AtomicUsize,
}

impl NoListBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            list_calls: AtomicUsize::new(0),
        }
    }

    fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl StorageBackend for NoListBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> arco_core::Result<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> arco_core::Result<WriteResult> {
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.list_calls.fetch_add(1, Ordering::SeqCst);
        Err(arco_core::Error::storage(format!(
            "list forbidden during control MVP request path: {prefix}"
        )))
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

async fn rewrite_object_pretty(storage: &ScopedStorage, path: &str) {
    let original = storage.get_raw(path).await.expect("object bytes");
    let json: Value = serde_json::from_slice(&original).expect("object json");
    storage
        .put_raw(
            path,
            Bytes::from(serde_json::to_vec_pretty(&json).expect("pretty json")),
            WritePrecondition::None,
        )
        .await
        .expect("rewrite object");
}
