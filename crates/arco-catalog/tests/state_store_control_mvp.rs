//! Object-store control-state MVP contract tests.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, CatalogError, CheckpointOptions,
    ControlMvpPaths, ControlMvpProjectionOutboxRecord, ControlMvpRestoreParticipant,
    ControlMvpStateStore, KeyRange, PersistedAuthorityAdapter, PersistedAuthorityKind,
    PersistedAuthorityReference, PersistedRestoreParticipantPlan, RestoreAttemptIdentity,
    RestoreParticipantInspection, StateRestoreParticipant, StateScope, TxnOptions,
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

#[test]
fn restore_paths_reject_separators_and_dot_segments_before_interpolation() {
    for domain in [".", "..", "../other", "a/b", r"a\b"] {
        let (_backend, storage) = storage();
        assert!(
            ControlMvpStateStore::new(storage, StateScope::new("tenant", "workspace", domain),)
                .is_err(),
            "unsafe domain {domain:?} must not reach ControlMvpPaths"
        );
        assert!(
            RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, domain).is_err(),
            "unsafe restore identity domain {domain:?} must fail"
        );
    }
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
    let source = store
        .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::hours(1))
        .await
        .expect("persist checkpoint reference");
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

    let mut newer = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin newer transaction");
    newer
        .put(b"catalog/default", Bytes::from_static(b"v2"))
        .await
        .expect("stage newer value");
    newer.commit().await.expect("commit newer value");
    let adapter = ControlMvpRestoreParticipant::new(store.clone());
    let plan = adapter
        .plan_restore(
            &source,
            &RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog")
                .expect("identity"),
            Utc::now(),
        )
        .await
        .expect("plan restore without listing");
    assert!(matches!(
        adapter
            .apply_restore(&plan, Utc::now())
            .await
            .expect("apply restore without listing"),
        RestoreParticipantInspection::Visible { .. }
    ));

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

async fn retained_v1_and_current_v2(store: &ControlMvpStateStore) -> PersistedAuthorityReference {
    let mut first = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin v1");
    first
        .put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("put v1");
    first
        .put(b"catalog/removed-later", Bytes::from_static(b"kept"))
        .await
        .expect("put retained key");
    first.commit().await.expect("commit v1");
    let checkpoint = store
        .checkpoint(CheckpointOptions::new(Some(scope())))
        .await
        .expect("checkpoint v1");
    let reference = store
        .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::hours(1))
        .await
        .expect("persist checkpoint");

    let mut second = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin v2");
    second
        .put(b"catalog/default", Bytes::from_static(b"v2"))
        .await
        .expect("put v2");
    second
        .delete(b"catalog/removed-later")
        .await
        .expect("delete retained key");
    second
        .put(b"catalog/newer-only", Bytes::from_static(b"new"))
        .await
        .expect("put newer key");
    second.commit().await.expect("commit v2");
    reference
}

#[tokio::test]
async fn restore_plan_is_deterministic_read_only_and_binds_both_pointer_digests() {
    let (backend, storage) = storage();
    let store = store(storage.clone());
    let source = retained_v1_and_current_v2(&store).await;
    let adapter = ControlMvpRestoreParticipant::new(store.clone());
    let identity = RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog")
        .expect("identity");
    let before = backend.list("").await.expect("inventory before").len();

    let first = adapter
        .plan_restore(&source, &identity, Utc::now())
        .await
        .expect("first plan");
    let second = adapter
        .plan_restore(&source, &identity, Utc::now())
        .await
        .expect("second plan");

    assert_eq!(first, second);
    let PersistedRestoreParticipantPlan::ControlMvp(plan) = &first;
    assert_eq!(3, plan.result_logical_sequence());
    assert!(plan.observed_base_pointer_sha256().starts_with("sha256:"));
    assert!(plan.candidate_pointer_sha256().starts_with("sha256:"));
    assert_ne!(
        plan.observed_base_pointer_sha256(),
        plan.candidate_pointer_sha256()
    );
    let serialized = serde_json::to_string(&first).expect("plan json");
    let round_trip: PersistedRestoreParticipantPlan =
        serde_json::from_str(&serialized).expect("plan round trip");
    assert_eq!(first, round_trip);
    assert_eq!(
        serialized,
        serde_json::to_string(&round_trip).expect("canonical re-encode")
    );
    assert_eq!(1, serialized.matches("\"plan_kind\"").count());
    assert_eq!(2, serialized.matches("\"implementation\"").count());
    assert!(!serialized.contains("StateToken"));
    assert!(!serialized.contains("CheckpointToken"));
    assert_eq!(
        before,
        backend.list("").await.expect("inventory after").len(),
        "read-only planning must not write"
    );
}

#[tokio::test]
async fn restore_plan_rejects_state_token_authority_without_writes() {
    let (backend, storage) = storage();
    let store = store(storage);
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("put");
    let token = txn.commit().await.expect("commit");
    let source = store
        .persist_state_reference(&token, Utc::now() + ChronoDuration::hours(1))
        .await
        .expect("state reference");
    let before = backend.list("").await.expect("before").len();
    let adapter = ControlMvpRestoreParticipant::new(store);
    let identity = RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog")
        .expect("identity");

    assert!(
        adapter
            .plan_restore(&source, &identity, Utc::now())
            .await
            .is_err()
    );
    assert_eq!(before, backend.list("").await.expect("after").len());
}

#[tokio::test]
async fn restore_apply_rolls_forward_and_preserves_historical_checkpoint() {
    let (_backend, storage) = storage();
    let control_store = store(storage);
    let source = retained_v1_and_current_v2(&control_store).await;
    let adapter = ControlMvpRestoreParticipant::new(control_store.clone());
    let identity = RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog")
        .expect("identity");
    let plan = adapter
        .plan_restore(&source, &identity, Utc::now())
        .await
        .expect("plan");

    assert!(matches!(
        adapter.inspect_restore(&plan).await.expect("inspect"),
        RestoreParticipantInspection::Ready
    ));
    let visible = adapter
        .apply_restore(&plan, Utc::now())
        .await
        .expect("apply");
    let RestoreParticipantInspection::Visible { token, evidence } = visible else {
        panic!("restore must become visible");
    };
    assert_eq!(3, token.logical_sequence());
    assert_eq!(3, evidence.logical_sequence());
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        control_store
            .get(b"catalog/default")
            .await
            .expect("current read")
    );
    assert_eq!(
        Some(Bytes::from_static(b"kept")),
        control_store
            .get(b"catalog/removed-later")
            .await
            .expect("restored read")
    );
    assert_eq!(
        None,
        control_store
            .get(b"catalog/newer-only")
            .await
            .expect("deleted")
    );
    let retained = control_store
        .resolve_persisted_reference(&source)
        .await
        .expect("historical reader");
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        retained.get(b"catalog/default").await.expect("old read")
    );
    assert!(matches!(
        adapter.inspect_restore(&plan).await.expect("reinspect"),
        RestoreParticipantInspection::Visible { .. }
    ));
}

#[tokio::test]
async fn restore_recovery_reconciles_pointer_write_then_transport_error() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(PointerWriteThenErrorBackend::new(inner));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = store(storage);
    let source = retained_v1_and_current_v2(&store).await;
    let adapter = ControlMvpRestoreParticipant::new(store);
    let identity = RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog")
        .expect("identity");
    let plan = adapter
        .plan_restore(&source, &identity, Utc::now())
        .await
        .expect("plan");
    backend.arm();

    assert!(matches!(
        adapter
            .apply_restore(&plan, Utc::now())
            .await
            .expect("reconciled apply"),
        RestoreParticipantInspection::Visible { .. }
    ));
}

#[tokio::test]
async fn restore_plan_rejects_corrupt_deterministic_identity_fields() {
    let (_backend, storage) = storage();
    let store = store(storage);
    let source = retained_v1_and_current_v2(&store).await;
    let adapter = ControlMvpRestoreParticipant::new(store);
    let identity = RestoreAttemptIdentity::new("rst_00000000000000000000000001", 1, "catalog")
        .expect("identity");
    let plan = adapter
        .plan_restore(&source, &identity, Utc::now())
        .await
        .expect("plan");
    let mut value = serde_json::to_value(&plan).expect("plan json");
    value["restore_outbox_record_id"] = Value::String("forged-outbox".to_string());
    let corrupt: PersistedRestoreParticipantPlan =
        serde_json::from_value(value).expect("plan shape");
    assert!(adapter.inspect_restore(&corrupt).await.is_err());

    for field in [
        "observed_base_pointer_sha256",
        "transaction_sha256",
        "candidate_manifest_sha256",
        "candidate_pointer_sha256",
    ] {
        let mut value = serde_json::to_value(&plan).expect("plan json");
        value[field] = Value::String(format!("sha256:{}", "f".repeat(64)));
        let corrupt: PersistedRestoreParticipantPlan =
            serde_json::from_value(value).expect("plan shape");
        assert!(
            adapter.inspect_restore(&corrupt).await.is_err(),
            "Ready inspection must deterministically reject corrupt {field} before metadata"
        );
    }

    let mut value = serde_json::to_value(&plan).expect("plan json");
    value["source"]["manifest_sha256"] = Value::String(format!("sha256:{}", "f".repeat(64)));
    let corrupt_source: PersistedRestoreParticipantPlan =
        serde_json::from_value(value).expect("plan shape");
    assert!(
        adapter.inspect_restore(&corrupt_source).await.is_err(),
        "source manifest evidence must participate in deterministic identity"
    );

    for (field, replacement) in [
        ("record_type", Value::String("other_plan".to_string())),
        ("version", Value::from(2_u64)),
    ] {
        let mut value = serde_json::to_value(&plan).expect("plan json");
        value[field] = replacement;
        let unsupported: PersistedRestoreParticipantPlan =
            serde_json::from_value(value).expect("plan shape");
        assert!(
            adapter.inspect_restore(&unsupported).await.is_err(),
            "unsupported nested restore plan {field} must fail validation"
        );
    }
}

#[tokio::test]
async fn restore_apply_resumes_transaction_and_manifest_crash_points() {
    for needle in ["/manifests/", "/current.pointer.json"] {
        let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let backend = Arc::new(FailOncePathBackend::new(inner, needle));
        let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
        let store = store(storage.clone());
        let source = retained_v1_and_current_v2(&store).await;
        let adapter = ControlMvpRestoreParticipant::new(store.clone());
        let plan = adapter
            .plan_restore(
                &source,
                &RestoreAttemptIdentity::new("rst_00000000000000000000000002", 1, "catalog")
                    .expect("identity"),
                Utc::now(),
            )
            .await
            .expect("plan");
        let PersistedRestoreParticipantPlan::ControlMvp(control) = &plan;
        backend.arm();
        assert!(
            adapter.apply_restore(&plan, Utc::now()).await.is_err(),
            "injected crash at {needle} must interrupt the first apply"
        );
        assert_eq!(
            2,
            store
                .current_state_token()
                .await
                .expect("base token")
                .logical_sequence(),
            "a pre-pointer crash cannot publish the restore"
        );
        assert!(storage.get_raw(control.transaction_path()).await.is_ok());
        if needle == "/manifests/" {
            assert!(
                storage
                    .get_raw(control.candidate_manifest_path())
                    .await
                    .is_err()
            );
        } else {
            assert!(
                storage
                    .get_raw(control.candidate_manifest_path())
                    .await
                    .is_ok()
            );
        }
        assert!(matches!(
            adapter
                .apply_restore(&plan, Utc::now())
                .await
                .expect("retry exact immutable writes"),
            RestoreParticipantInspection::Visible { .. }
        ));
    }
}

#[tokio::test]
async fn restore_foreign_pointer_is_superseded_and_later_lineage_stays_visible() {
    {
        let (_backend, storage) = storage();
        let store = store(storage);
        let source = retained_v1_and_current_v2(&store).await;
        let adapter = ControlMvpRestoreParticipant::new(store.clone());
        let plan = adapter
            .plan_restore(
                &source,
                &RestoreAttemptIdentity::new("rst_00000000000000000000000003", 1, "catalog")
                    .expect("identity"),
                Utc::now(),
            )
            .await
            .expect("plan");
        let mut foreign = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("foreign transaction");
        foreign
            .put(b"catalog/foreign", Bytes::from_static(b"winner"))
            .await
            .expect("foreign write");
        let winner = foreign.commit().await.expect("foreign commit");
        assert!(matches!(
            adapter.inspect_restore(&plan).await.expect("inspect"),
            RestoreParticipantInspection::Superseded
        ));
        assert!(matches!(
            adapter
                .apply_restore(&plan, Utc::now())
                .await
                .expect("superseded apply"),
            RestoreParticipantInspection::Superseded
        ));
        assert_eq!(
            winner.logical_sequence(),
            store
                .current_state_token()
                .await
                .expect("current winner")
                .logical_sequence()
        );
    }

    {
        let (_backend, storage) = storage();
        let store = store(storage);
        let source = retained_v1_and_current_v2(&store).await;
        let adapter = ControlMvpRestoreParticipant::new(store.clone());
        let restore_id = "rst_00000000000000000000000004";
        let plan = adapter
            .plan_restore(
                &source,
                &RestoreAttemptIdentity::new(restore_id, 1, "catalog").expect("identity"),
                Utc::now(),
            )
            .await
            .expect("plan");
        assert!(matches!(
            adapter
                .apply_restore(&plan, Utc::now())
                .await
                .expect("visible restore"),
            RestoreParticipantInspection::Visible { .. }
        ));
        let mut later = store
            .begin_control_txn(TxnOptions::default())
            .await
            .expect("later transaction");
        later
            .put(b"catalog/later", Bytes::from_static(b"still-visible"))
            .await
            .expect("later write");
        later.commit().await.expect("later commit");
        assert!(matches!(
            adapter
                .inspect_restore(&plan)
                .await
                .expect("lineage inspect"),
            RestoreParticipantInspection::Visible { .. }
        ));
        let outbox = store
            .current_projection_outbox()
            .await
            .expect("visible outbox");
        assert_eq!(1, outbox.len());
        assert_eq!(
            format!("restore:{restore_id}:1:catalog"),
            outbox[0].record_id()
        );
    }
}

#[tokio::test]
async fn restore_preflight_before_mutation_rejects_corrupt_expired_unstable_and_overflow() {
    let (backend, storage) = storage();
    let control_store = store(storage);
    let source = retained_v1_and_current_v2(&control_store).await;
    let adapter = ControlMvpRestoreParticipant::new(control_store.clone());
    let identity = RestoreAttemptIdentity::new("rst_00000000000000000000000005", 1, "catalog")
        .expect("identity");
    let before = backend.list("").await.expect("inventory before").len();

    assert!(
        adapter
            .plan_restore(
                &source,
                &identity,
                source.retention_deadline() + ChronoDuration::seconds(1),
            )
            .await
            .is_err(),
        "expired source must fail before planning metadata"
    );
    for (field, replacement) in [
        ("implementation", Value::String("other-backend".to_string())),
        ("logical_sequence", Value::from(999_u64)),
        (
            "manifest_sha256",
            Value::String(format!("sha256:{}", "f".repeat(64))),
        ),
    ] {
        let mut value = serde_json::to_value(&source).expect("source json");
        value[field] = replacement;
        let corrupt: PersistedAuthorityReference =
            serde_json::from_value(value).expect("source shape");
        assert!(
            adapter
                .plan_restore(&corrupt, &identity, Utc::now())
                .await
                .is_err(),
            "corrupt {field} must fail before planning metadata"
        );
    }
    let mut wrong_scope = serde_json::to_value(&source).expect("source json");
    wrong_scope["scope"]["workspace_id"] = Value::String("other-workspace".to_string());
    let wrong_scope: PersistedAuthorityReference =
        serde_json::from_value(wrong_scope).expect("source shape");
    assert!(
        adapter
            .plan_restore(&wrong_scope, &identity, Utc::now())
            .await
            .is_err(),
        "out-of-scope authority must fail before planning metadata"
    );

    let valid_plan = adapter
        .plan_restore(&source, &identity, Utc::now())
        .await
        .expect("valid plan");
    let mut overflow = serde_json::to_value(&valid_plan).expect("plan json");
    overflow["base_logical_sequence"] = Value::from(u64::MAX);
    overflow["result_logical_sequence"] = Value::from(0_u64);
    let overflow: PersistedRestoreParticipantPlan =
        serde_json::from_value(overflow).expect("overflow plan shape");
    assert!(adapter.inspect_restore(&overflow).await.is_err());
    assert_eq!(
        before,
        backend.list("").await.expect("inventory after").len()
    );

    let inner = Arc::new(MemoryBackend::new());
    let setup_storage =
        ScopedStorage::new(inner.clone(), "tenant", "workspace").expect("setup storage");
    let setup_store = store(setup_storage);
    let unstable_source = retained_v1_and_current_v2(&setup_store).await;
    let before = inner
        .list("")
        .await
        .expect("unstable inventory before")
        .len();
    let unstable_backend = Arc::new(UnstablePointerHeadBackend::new(inner.clone()));
    let unstable_storage =
        ScopedStorage::new(unstable_backend, "tenant", "workspace").expect("unstable storage");
    let unstable_adapter = ControlMvpRestoreParticipant::new(store(unstable_storage));
    assert!(matches!(
        unstable_adapter
            .plan_restore(&unstable_source, &identity, Utc::now())
            .await,
        Err(CatalogError::CasFailed { .. })
    ));
    assert_eq!(
        before,
        inner
            .list("")
            .await
            .expect("unstable inventory after")
            .len()
    );
}

#[tokio::test]
async fn restore_empty_current_base_extends_source_lineage_and_retries_idempotently() {
    let (_backend, storage) = storage();
    let store = store(storage.clone());
    let source = retained_v1_and_current_v2(&store).await;
    storage
        .delete(&store.paths().current_pointer())
        .await
        .expect("remove current pointer while retaining source lineage");
    let adapter = ControlMvpRestoreParticipant::new(store.clone());
    let plan = adapter
        .plan_restore(
            &source,
            &RestoreAttemptIdentity::new("rst_00000000000000000000000008", 1, "catalog")
                .expect("identity"),
            Utc::now(),
        )
        .await
        .expect("plan from explicit empty current base");
    let PersistedRestoreParticipantPlan::ControlMvp(control) = &plan;
    assert_eq!(
        source.logical_sequence() + 1,
        control.result_logical_sequence()
    );
    let wire = serde_json::to_value(&plan).expect("plan wire");
    assert_eq!("empty", wire["current_base_kind"]);
    assert!(wire["base_pointer_version"].is_null());
    assert!(matches!(
        adapter.inspect_restore(&plan).await.expect("ready inspect"),
        RestoreParticipantInspection::Ready
    ));

    let first = adapter
        .apply_restore(&plan, Utc::now())
        .await
        .expect("empty-base apply");
    let RestoreParticipantInspection::Visible { token, .. } = first else {
        panic!("empty-base restore must become visible");
    };
    assert_eq!(source.logical_sequence() + 1, token.logical_sequence());
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        ArcoStateReader::get(&store, b"catalog/default")
            .await
            .expect("restored source value")
    );
    assert!(matches!(
        adapter
            .apply_restore(&plan, Utc::now())
            .await
            .expect("idempotent retry"),
        RestoreParticipantInspection::Visible { .. }
    ));
}

#[tokio::test]
async fn restore_empty_current_base_competing_first_writer_is_superseded_without_overwrite() {
    let (_backend, storage) = storage();
    let store = store(storage.clone());
    let source = retained_v1_and_current_v2(&store).await;
    storage
        .delete(&store.paths().current_pointer())
        .await
        .expect("remove current pointer while retaining source lineage");
    let adapter = ControlMvpRestoreParticipant::new(store.clone());
    let plan = adapter
        .plan_restore(
            &source,
            &RestoreAttemptIdentity::new("rst_00000000000000000000000009", 1, "catalog")
                .expect("identity"),
            Utc::now(),
        )
        .await
        .expect("empty-base plan");

    let mut competing = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("competing first writer");
    competing
        .put(b"catalog/foreign", Bytes::from_static(b"winner"))
        .await
        .expect("foreign write");
    competing.commit().await.expect("publish foreign pointer");
    let pointer_path = store.paths().current_pointer();
    let winner_pointer = storage
        .get_raw(&pointer_path)
        .await
        .expect("foreign winner pointer");

    assert!(matches!(
        adapter
            .inspect_restore(&plan)
            .await
            .expect("superseded inspect"),
        RestoreParticipantInspection::Superseded
    ));
    assert!(matches!(
        adapter
            .apply_restore(&plan, Utc::now())
            .await
            .expect("superseded apply"),
        RestoreParticipantInspection::Superseded
    ));
    assert_eq!(
        winner_pointer,
        storage
            .get_raw(&pointer_path)
            .await
            .expect("winner pointer remains")
    );
    assert_eq!(
        Some(Bytes::from_static(b"winner")),
        ArcoStateReader::get(&store, b"catalog/foreign")
            .await
            .expect("foreign winner remains visible")
    );
}

#[tokio::test]
async fn restore_immutable_object_conflicts_never_publish_pointer() {
    let (_backend, first_storage) = storage();
    let first_store = store(first_storage.clone());
    let source = retained_v1_and_current_v2(&first_store).await;
    let adapter = ControlMvpRestoreParticipant::new(first_store.clone());
    let plan = adapter
        .plan_restore(
            &source,
            &RestoreAttemptIdentity::new("rst_00000000000000000000000006", 1, "catalog")
                .expect("identity"),
            Utc::now(),
        )
        .await
        .expect("plan");
    let PersistedRestoreParticipantPlan::ControlMvp(control) = &plan;
    first_storage
        .put_raw(
            control.transaction_path(),
            Bytes::from_static(b"{}"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("conflicting immutable transaction");
    assert!(adapter.apply_restore(&plan, Utc::now()).await.is_err());
    assert_eq!(
        2,
        first_store
            .current_state_token()
            .await
            .expect("current token")
            .logical_sequence()
    );
    assert!(
        first_storage
            .get_raw(control.candidate_manifest_path())
            .await
            .is_err(),
        "transaction conflict must stop before candidate manifest publication"
    );

    let (_backend, storage) = storage();
    let store = store(storage.clone());
    let source = retained_v1_and_current_v2(&store).await;
    let adapter = ControlMvpRestoreParticipant::new(store.clone());
    let plan = adapter
        .plan_restore(
            &source,
            &RestoreAttemptIdentity::new("rst_00000000000000000000000007", 1, "catalog")
                .expect("identity"),
            Utc::now(),
        )
        .await
        .expect("manifest-conflict plan");
    let PersistedRestoreParticipantPlan::ControlMvp(control) = &plan;
    storage
        .put_raw(
            control.candidate_manifest_path(),
            Bytes::from_static(b"{}"),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("conflicting immutable manifest");
    assert!(adapter.apply_restore(&plan, Utc::now()).await.is_err());
    assert_eq!(
        2,
        store
            .current_state_token()
            .await
            .expect("current token")
            .logical_sequence(),
        "manifest conflict must leave the prior pointer visible"
    );
}

struct PointerWriteThenErrorBackend {
    inner: Arc<dyn StorageBackend>,
    armed: AtomicBool,
}

struct FailOncePathBackend {
    inner: Arc<dyn StorageBackend>,
    needle: String,
    armed: AtomicBool,
}

impl FailOncePathBackend {
    fn new(inner: Arc<dyn StorageBackend>, needle: &str) -> Self {
        Self {
            inner,
            needle: needle.to_string(),
            armed: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for FailOncePathBackend {
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
        if path.contains(&self.needle) && self.armed.swap(false, Ordering::SeqCst) {
            return Err(arco_core::Error::storage("injected restore crash point"));
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

struct UnstablePointerHeadBackend {
    inner: Arc<dyn StorageBackend>,
    counter: AtomicUsize,
}

impl UnstablePointerHeadBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            counter: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StorageBackend for UnstablePointerHeadBackend {
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
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        let mut meta = self.inner.head(path).await?;
        if path.ends_with("/current.pointer.json")
            && let Some(meta) = &mut meta
        {
            meta.version = format!("unstable-{}", self.counter.fetch_add(1, Ordering::SeqCst));
        }
        Ok(meta)
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

impl PointerWriteThenErrorBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for PointerWriteThenErrorBackend {
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
        let result = self.inner.put(path, data, precondition).await?;
        if path.ends_with("/current.pointer.json") && self.armed.swap(false, Ordering::SeqCst) {
            return Err(arco_core::Error::storage(
                "injected transport error after pointer write",
            ));
        }
        Ok(result)
    }

    async fn delete(&self, path: &str) -> arco_core::Result<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> arco_core::Result<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}
