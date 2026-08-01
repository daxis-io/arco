//! Object-store control-state MVP contract tests.

use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arco_catalog::{
    ArcoStateAdmin, ArcoStateReader, ArcoStateTxn, CatalogError, CheckpointOptions,
    ControlMvpOutboxTrimTarget, ControlMvpPaths, ControlMvpProjectionOutboxRecord,
    ControlMvpRestoreParticipant, ControlMvpStateStore, KeyRange, PersistedAuthorityAdapter,
    PersistedAuthorityKind, PersistedAuthorityReference, PersistedRestoreParticipantPlan,
    RestoreAttemptIdentity, RestoreParticipantInspection, StateRestoreParticipant, StateScope,
    TxnOptions,
};
use arco_core::storage::{ObjectMeta, StorageBackend, WritePrecondition, WriteResult};
use arco_core::{MemoryBackend, ScopedStorage};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use sha2::Digest;

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
    stale_txn
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "stale-outbox",
            Bytes::from_static(b"stale"),
        ))
        .expect("stage outbox record");
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
    winning_txn
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "winning-outbox",
            Bytes::from_static(b"winner"),
        ))
        .expect("stage outbox record");
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
        vec![ControlMvpProjectionOutboxRecord::with_origin_sequence(
            "winning-outbox",
            Bytes::from_static(b"winner"),
            1,
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
    stale_txn
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "hidden-outbox",
            Bytes::from_static(b"hidden"),
        ))
        .expect("stage outbox record");

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
    first_txn
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "first",
            Bytes::from_static(b"payload-1"),
        ))
        .expect("stage outbox record");
    let first_token = first_txn.commit().await.expect("commit first");

    let mut stale_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin stale transaction");
    stale_txn
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "stale",
            Bytes::from_static(b"payload-stale"),
        ))
        .expect("stage outbox record");

    let mut winning_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin winning transaction");
    winning_txn
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "second",
            Bytes::from_static(b"payload-2"),
        ))
        .expect("stage outbox record");
    let second_token = winning_txn.commit().await.expect("commit second");
    assert!(matches!(
        stale_txn.commit().await,
        Err(CatalogError::CasFailed { .. })
    ));

    assert_eq!(
        vec![ControlMvpProjectionOutboxRecord::with_origin_sequence(
            "first",
            Bytes::from_static(b"payload-1"),
            1,
        )],
        store
            .projection_outbox_at(first_token)
            .await
            .expect("first outbox")
    );
    assert_eq!(
        vec![
            ControlMvpProjectionOutboxRecord::with_origin_sequence(
                "first",
                Bytes::from_static(b"payload-1"),
                1,
            ),
            ControlMvpProjectionOutboxRecord::with_origin_sequence(
                "second",
                Bytes::from_static(b"payload-2"),
                2,
            ),
        ],
        store
            .projection_outbox_at(second_token)
            .await
            .expect("second outbox")
    );
    assert_eq!(
        vec![
            ControlMvpProjectionOutboxRecord::with_origin_sequence(
                "first",
                Bytes::from_static(b"payload-1"),
                1,
            ),
            ControlMvpProjectionOutboxRecord::with_origin_sequence(
                "second",
                Bytes::from_static(b"payload-2"),
                2,
            ),
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
    let store = store(storage.clone());

    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"v1"))
        .await
        .expect("stage write");
    txn.put(b"catalog/other", Bytes::from_static(b"retained"))
        .await
        .expect("stage second key");
    let checkpointed_token = txn.commit().await.expect("commit");
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
        .read_checkpoint(checkpoint.clone())
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

    // A value read proves the reader opened *something*. What the checkpoint
    // contract owes is that the reader serves the whole state the checkpoint's
    // authority manifest asserts, so compare it against a token-pinned read of
    // that manifest rather than against one key.
    let manifest_reader = store
        .read_at(checkpointed_token.clone())
        .await
        .expect("token-pinned read of the checkpointed manifest");
    assert_eq!(
        manifest_reader
            .scan_prefix(b"")
            .await
            .expect("manifest state"),
        checkpoint_reader
            .scan_prefix(b"")
            .await
            .expect("checkpoint state"),
        "the checkpoint reader must serve exactly the manifest-named state"
    );

    // And the snapshot the checkpoint names is bound by raw-byte checksum, so
    // substituting its bytes fails closed instead of serving another state.
    let paths = ControlMvpPaths::new("catalog");
    let checkpoint_path = paths.checkpoint_object(checkpoint.checkpoint_id());
    let state_id = envelope_payload(&storage, &checkpoint_path).await["state"]["state_id"]
        .as_str()
        .expect("checkpoint state id")
        .to_string();
    let snapshot_path = paths.state_object(&state_id);
    let snapshot = storage
        .get_raw(&snapshot_path)
        .await
        .expect("checkpoint snapshot");
    storage
        .put_raw(
            &snapshot_path,
            reseal_envelope(&snapshot, |payload| payload.replace("[118,49]", "[118,50]")),
            WritePrecondition::None,
        )
        .await
        .expect("substitute the snapshot bytes");
    let error = store
        .read_checkpoint(checkpoint)
        .await
        .err()
        .expect("a substituted checkpoint snapshot must fail closed");
    assert!(
        matches!(error, CatalogError::InvariantViolation { .. }),
        "unexpected error: {error:?}"
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
        2,
        plan.version(),
        "planning writes the current plan version"
    );
    assert!(!plan.is_legacy_version());

    // R6: a round trip of the version this revision writes proves only that
    // the writer and reader agree with themselves. Recovery has to read plans
    // written by *older* revisions, so the decoder is exercised across the
    // version boundary in both directions here.
    let mut downgraded = serde_json::from_str::<Value>(&serialized).expect("plan value");
    let removed = downgraded
        .as_object_mut()
        .expect("plan object")
        .remove("observed_writer_epoch");
    assert_eq!(Some(Value::from(0_u64)), removed);
    downgraded["version"] = Value::from(1_u64);
    let migrated: PersistedRestoreParticipantPlan =
        serde_json::from_value(downgraded.clone()).expect("v1 plans must remain decodable");
    let PersistedRestoreParticipantPlan::ControlMvp(migrated_plan) = &migrated;
    assert_eq!(1, migrated_plan.version());
    assert!(
        migrated_plan.is_legacy_version(),
        "a decoded v1 plan must stay marked legacy so it can never be applied"
    );
    assert_eq!(
        plan.transaction_sha256(),
        migrated_plan.transaction_sha256(),
        "the migration must preserve every field v1 did carry"
    );

    // The current version, in contrast, still requires the field: an absent
    // observation must never be silently read as an observation of epoch 0.
    let mut truncated = downgraded;
    truncated["version"] = Value::from(2_u64);
    let error = serde_json::from_value::<PersistedRestoreParticipantPlan>(truncated)
        .expect_err("a v2 plan without observed_writer_epoch must fail closed");
    assert!(
        error.to_string().contains("observed_writer_epoch"),
        "unexpected error: {error}"
    );

    assert_eq!(
        before,
        backend.list("").await.expect("inventory after").len(),
        "read-only planning must not write"
    );
}

/// R6: literal, hand-maintained versioned plan fixtures.
///
/// These files are authored by hand and must never be regenerated with the
/// current serializers: a fixture produced by today's code proves only that
/// today's code agrees with itself. `v1_pre_observed_writer_epoch.json` is the
/// shape an older revision durably wrote, and `v2_current.json` is the shape
/// this revision writes. Together they pin the exact accepted/rejected
/// compatibility policy the recovery path depends on.
#[test]
fn literal_versioned_restore_plan_fixtures_pin_the_compatibility_policy() {
    let v1 = include_str!("fixtures/control_mvp_restore_plans/v1_pre_observed_writer_epoch.json");
    let v2 = include_str!("fixtures/control_mvp_restore_plans/v2_current.json");
    let v1_value: Value = serde_json::from_str(v1).expect("v1 fixture json");
    let v2_value: Value = serde_json::from_str(v2).expect("v2 fixture json");

    // The only shape difference between the two versions is the field that
    // version 1 predates.
    assert_eq!(Value::from(1_u64), v1_value["version"]);
    assert_eq!(Value::from(2_u64), v2_value["version"]);
    assert!(v1_value.get("observed_writer_epoch").is_none());
    assert!(v2_value["observed_writer_epoch"].is_u64());
    let field_names = |value: &Value| {
        let mut names = value
            .as_object()
            .expect("fixture object")
            .keys()
            .filter(|name| name.as_str() != "observed_writer_epoch")
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        names
    };
    assert_eq!(
        field_names(&v1_value),
        field_names(&v2_value),
        "the versions must differ only by observed_writer_epoch"
    );

    // ACCEPTED: version 1 decodes by explicit migration and stays marked
    // legacy; version 2 decodes normally.
    let PersistedRestoreParticipantPlan::ControlMvp(migrated) =
        serde_json::from_str(v1).expect("v1 fixture must not fail deserialization")
    else {
        panic!("unexpected plan kind")
    };
    assert_eq!(1, migrated.version());
    assert!(migrated.is_legacy_version());
    assert_eq!(3, migrated.result_logical_sequence());
    assert_eq!(
        "sha256:1193f69c41dccf7cc6693a2ad70a991ef5213c5ab7ac331c55129fed51791e7e",
        migrated.transaction_sha256()
    );
    let PersistedRestoreParticipantPlan::ControlMvp(current) =
        serde_json::from_str(v2).expect("v2 fixture must decode")
    else {
        panic!("unexpected plan kind")
    };
    assert_eq!(2, current.version());
    assert!(!current.is_legacy_version());
    assert_eq!(
        migrated.transaction_sha256(),
        current.transaction_sha256(),
        "the migration must preserve every field version 1 did carry"
    );

    // REJECTED: a version 2 record missing the field it is required to carry,
    // and a version 1 record carrying the field it never wrote. Neither may be
    // guessed at, because "absent" and "observed epoch 0" are different facts.
    let mut truncated_v2 = v2_value.clone();
    truncated_v2
        .as_object_mut()
        .expect("v2 object")
        .remove("observed_writer_epoch");
    let error = serde_json::from_value::<PersistedRestoreParticipantPlan>(truncated_v2)
        .expect_err("a v2 plan without observed_writer_epoch must fail closed");
    assert!(
        error.to_string().contains("observed_writer_epoch"),
        "unexpected error: {error}"
    );
    let mut contradictory_v1 = v1_value;
    contradictory_v1["observed_writer_epoch"] = Value::from(7_u64);
    let error = serde_json::from_value::<PersistedRestoreParticipantPlan>(contradictory_v1)
        .expect_err("a v1 record with observed_writer_epoch must fail closed");
    assert!(
        error.to_string().contains("observed_writer_epoch"),
        "unexpected error: {error}"
    );
}

/// R6: the runtime half of the compatibility policy, run against a *matching*
/// retained source so the outcomes discriminate.
///
/// A live plan that inspects Ready is downgraded to the checked-in version 1
/// shape. The downgraded plan therefore describes exactly the lineage that
/// would otherwise be applied — so the only reason it must not be applied is
/// its version. It has to reach a defined terminal outcome and write nothing.
#[tokio::test]
async fn a_v1_plan_over_a_matching_source_is_superseded_and_never_applied() {
    let (backend, storage) = storage();
    let store = store(storage);
    let source = retained_v1_and_current_v2(&store).await;
    let adapter = ControlMvpRestoreParticipant::new(store.clone());
    let plan = adapter
        .plan_restore(
            &source,
            &RestoreAttemptIdentity::new("rst_00000000000000000000000004", 1, "catalog")
                .expect("identity"),
            Utc::now(),
        )
        .await
        .expect("plan restore");

    // Positive control: at the current version this exact plan is Ready.
    assert!(
        matches!(
            adapter
                .inspect_restore(&plan)
                .await
                .expect("inspect current-version plan"),
            RestoreParticipantInspection::Ready
        ),
        "the fixture harness must be able to reach Ready, or Superseded proves nothing"
    );

    // Downgrade it to the checked-in version 1 shape.
    let mut wire = serde_json::to_value(&plan).expect("plan json");
    let object = wire.as_object_mut().expect("plan object");
    object.remove("observed_writer_epoch");
    object.insert("version".to_string(), Value::from(1_u64));
    let fixture: Value = serde_json::from_str(include_str!(
        "fixtures/control_mvp_restore_plans/v1_pre_observed_writer_epoch.json"
    ))
    .expect("v1 fixture json");
    let names = |value: &Value| {
        let mut names = value
            .as_object()
            .expect("object")
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        names
    };
    assert_eq!(
        names(&fixture),
        names(&wire),
        "the downgrade must reproduce the checked-in v1 field set exactly"
    );
    let legacy: PersistedRestoreParticipantPlan =
        serde_json::from_value(wire).expect("the downgraded plan must remain decodable");

    // Defined terminal outcome, and no writes: a legacy plan is superseded so
    // the driver replans it, and it never becomes authority.
    let inventory_before = backend.list("").await.expect("inventory before").len();
    assert!(
        matches!(
            adapter
                .inspect_restore(&legacy)
                .await
                .expect("inspect legacy plan"),
            RestoreParticipantInspection::Superseded
        ),
        "a legacy plan must reach a defined terminal outcome, not an error"
    );
    assert!(
        matches!(
            adapter
                .apply_restore(&legacy, Utc::now())
                .await
                .expect("apply legacy plan"),
            RestoreParticipantInspection::Superseded
        ),
        "a legacy plan must never be applied"
    );
    assert_eq!(
        inventory_before,
        backend.list("").await.expect("inventory after").len(),
        "a legacy plan must not write anything"
    );
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        store
            .get(b"catalog/default")
            .await
            .expect("current authority is untouched")
    );

    // The current-version plan still applies, so the refusal is version-scoped
    // rather than a blanket refusal.
    assert!(matches!(
        adapter
            .apply_restore(&plan, Utc::now())
            .await
            .expect("apply current-version plan"),
        RestoreParticipantInspection::Visible { .. }
    ));
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        store.get(b"catalog/default").await.expect("restored value")
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
        ("version", Value::from(3_u64)),
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

    // Version 1 is deliberately decodable for recovery compatibility, but it
    // predates writer-epoch evidence and therefore can only be inspected as a
    // terminal legacy plan, never applied.
    let mut value = serde_json::to_value(&plan).expect("plan json");
    value["version"] = Value::from(1_u64);
    value
        .as_object_mut()
        .expect("plan object")
        .remove("observed_writer_epoch");
    let legacy: PersistedRestoreParticipantPlan =
        serde_json::from_value(value).expect("legacy plan remains decodable");
    assert_eq!(
        RestoreParticipantInspection::Superseded,
        adapter
            .inspect_restore(&legacy)
            .await
            .expect("legacy plan has a defined terminal inspection"),
        "legacy plan without writer-epoch evidence must never become applicable"
    );
}

/// Fails the next write whose path contains the current scripted needle, then
/// advances to the next needle. Unlike a fail-once injection, this walks a
/// crash *sequence* through one plan and one storage.
struct ScriptedFailBackend {
    inner: Arc<dyn StorageBackend>,
    script: Vec<String>,
    position: AtomicUsize,
    armed: AtomicBool,
}

impl ScriptedFailBackend {
    fn new(inner: Arc<dyn StorageBackend>, script: &[&str]) -> Self {
        Self {
            inner,
            script: script.iter().map(|needle| (*needle).to_string()).collect(),
            position: AtomicUsize::new(0),
            armed: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn faults_injected(&self) -> usize {
        self.position.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl StorageBackend for ScriptedFailBackend {
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
        if self.armed.load(Ordering::SeqCst) {
            let position = self.position.load(Ordering::SeqCst);
            if let Some(needle) = self.script.get(position)
                && path.contains(needle.as_str())
            {
                self.position.store(position + 1, Ordering::SeqCst);
                return Err(arco_core::Error::storage(format!(
                    "injected restore fault at step {position}"
                )));
            }
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

/// One fail-once injection per isolated scenario only proves a restore can
/// survive *a* crash. Recovery has to survive a *sequence* of interrupted
/// retries against the same plan and the same storage, with the pre-restore
/// authority visible throughout and exactly the expected immutable prefix
/// materialized after each step.
#[tokio::test]
async fn restore_apply_survives_a_sequence_of_interrupted_retries() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(ScriptedFailBackend::new(
        inner,
        &["/txlog/tx-restore-", "/manifests/", "/current.pointer.json"],
    ));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = store(storage.clone());
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
        .expect("plan restore");
    let PersistedRestoreParticipantPlan::ControlMvp(control_plan) = &plan;
    let transaction_path = control_plan.transaction_path().to_string();
    let manifest_path = control_plan.candidate_manifest_path().to_string();
    let pre_restore_token = store
        .current_state_token()
        .await
        .expect("pre-restore token");
    let restore_notice_id = format!(
        "restore:{}:{}:{}",
        control_plan.identity().restore_id(),
        control_plan.identity().attempt(),
        control_plan.identity().domain()
    );

    // Immutable prefix each attempt must find on entry: attempt 0 is
    // interrupted writing the transaction, attempt 1 writes it and is
    // interrupted writing the manifest, attempt 2 writes that and is
    // interrupted at the pointer CAS.
    let expected_prefix = [(false, false), (false, false), (true, false)];
    backend.arm();
    for (attempt, (transaction_present, manifest_present)) in
        expected_prefix.into_iter().enumerate()
    {
        assert_eq!(
            transaction_present,
            storage.get_raw(&transaction_path).await.is_ok(),
            "attempt {attempt}: unexpected restore transaction presence before the attempt"
        );
        assert_eq!(
            manifest_present,
            storage.get_raw(&manifest_path).await.is_ok(),
            "attempt {attempt}: unexpected candidate manifest presence before the attempt"
        );

        let error = adapter
            .apply_restore(&plan, Utc::now())
            .await
            .err()
            .unwrap_or_else(|| panic!("attempt {attempt} must be interrupted"));
        assert!(
            matches!(error, CatalogError::Storage { .. }),
            "attempt {attempt}: unexpected error: {error:?}"
        );

        // The pre-restore authority stays visible after every injected fault.
        assert_eq!(
            pre_restore_token,
            store
                .current_state_token()
                .await
                .expect("token after injected fault"),
            "attempt {attempt}: an interrupted restore must not move the pointer"
        );
        assert_eq!(
            Some(Bytes::from_static(b"v2")),
            store
                .get(b"catalog/default")
                .await
                .expect("visible value after injected fault"),
            "attempt {attempt}: an interrupted restore must not become visible"
        );
    }
    assert_eq!(3, backend.faults_injected(), "the whole script must fire");
    assert!(
        storage.get_raw(&transaction_path).await.is_ok()
            && storage.get_raw(&manifest_path).await.is_ok(),
        "the interrupted pointer CAS must leave both immutable objects durable"
    );

    // Fourth attempt: nothing is left to fail, so the restore becomes visible.
    let inspection = adapter
        .apply_restore(&plan, Utc::now())
        .await
        .expect("the fourth attempt completes");
    let RestoreParticipantInspection::Visible { token, evidence } = inspection else {
        panic!("the fourth attempt must publish one visible restore");
    };
    assert_eq!(
        control_plan.result_logical_sequence(),
        token.logical_sequence()
    );
    assert_eq!(control_plan.candidate_manifest_id(), evidence.manifest_id());
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        store.get(b"catalog/default").await.expect("restored value")
    );
    assert_eq!(
        Some(Bytes::from_static(b"kept")),
        store
            .get(b"catalog/removed-later")
            .await
            .expect("key deleted after the source is restored")
    );
    assert_eq!(
        None,
        store
            .get(b"catalog/newer-only")
            .await
            .expect("key absent from the source is removed"),
    );

    // Exactly one restore notice was staged, and re-applying is idempotent.
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("current outbox");
    let notices = outbox
        .iter()
        .filter(|record| record.record_id() == restore_notice_id)
        .count();
    assert_eq!(1, notices, "a restore must stage exactly one notice");
    let repeat = adapter
        .apply_restore(&plan, Utc::now())
        .await
        .expect("re-applying a visible restore is idempotent");
    assert!(matches!(
        repeat,
        RestoreParticipantInspection::Visible { .. }
    ));
    assert_eq!(
        1,
        store
            .current_projection_outbox()
            .await
            .expect("outbox after repeat")
            .iter()
            .filter(|record| record.record_id() == restore_notice_id)
            .count()
    );
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

struct CountingGetBackend {
    inner: Arc<dyn StorageBackend>,
    get_calls: AtomicUsize,
}

impl CountingGetBackend {
    fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            get_calls: AtomicUsize::new(0),
        }
    }

    fn get_calls(&self) -> usize {
        self.get_calls.load(Ordering::SeqCst)
    }

    fn reset(&self) {
        self.get_calls.store(0, Ordering::SeqCst);
    }
}

#[async_trait]
impl StorageBackend for CountingGetBackend {
    async fn get(&self, path: &str) -> arco_core::Result<Bytes> {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
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
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn interval(value: u64) -> NonZeroU64 {
    NonZeroU64::new(value).expect("non-zero checkpoint interval")
}

async fn commit_value(store: &ControlMvpStateStore, key: &[u8], value: &str) {
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.put(key, Bytes::from(value.to_string()))
        .await
        .expect("stage write");
    txn.commit().await.expect("commit");
}

#[tokio::test]
async fn replay_after_anchor_is_bounded_independent_of_history_length() {
    let backend = Arc::new(CountingGetBackend::new(Arc::new(MemoryBackend::new())));
    let storage =
        ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("scoped storage");
    let store = ControlMvpStateStore::new(storage, scope())
        .expect("control MVP store")
        .with_checkpoint_interval(interval(4));

    for index in 0..13 {
        commit_value(&store, b"catalog/default", &format!("v{index}")).await;
    }

    backend.reset();
    assert_eq!(
        Some(Bytes::from_static(b"v12")),
        store.get(b"catalog/default").await.expect("bounded read")
    );
    let gets_at_short_history = backend.get_calls();

    for index in 13..25 {
        commit_value(&store, b"catalog/default", &format!("v{index}")).await;
    }

    backend.reset();
    assert_eq!(
        Some(Bytes::from_static(b"v24")),
        store.get(b"catalog/default").await.expect("bounded read")
    );
    let gets_at_long_history = backend.get_calls();

    assert_eq!(
        gets_at_short_history, gets_at_long_history,
        "read cost must not grow with pre-anchor history"
    );
    assert!(
        gets_at_long_history <= 3 + 4,
        "read cost {gets_at_long_history} must stay within pointer + manifest + snapshot + interval"
    );

    backend.reset();
    commit_value(&store, b"catalog/default", "v25").await;
    let gets_for_commit = backend.get_calls();
    assert!(
        gets_for_commit <= 3 + 4 + 1,
        "commit read cost {gets_for_commit} must stay bounded by the checkpoint interval"
    );

    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .expect("checkpoint after long history");
    backend.reset();
    let reader = store
        .read_checkpoint(checkpoint)
        .await
        .expect("bounded checkpoint read");
    assert_eq!(
        Some(Bytes::from_static(b"v25")),
        reader
            .get(b"catalog/default")
            .await
            .expect("checkpoint value")
    );
    assert!(
        backend.get_calls() <= 3,
        "checkpoint reads must load only the checkpoint, authority manifest, and snapshot"
    );
}

#[tokio::test]
async fn manifest_suffix_and_size_stay_bounded_by_checkpoint_interval() {
    let (_backend, storage) = storage();
    let store = ControlMvpStateStore::new(storage.clone(), scope())
        .expect("control MVP store")
        .with_checkpoint_interval(interval(4));
    let paths = ControlMvpPaths::new("catalog");

    let mut boundary_manifest_sizes = Vec::new();
    for index in 0..20 {
        commit_value(&store, b"catalog/default", &format!("v{index}")).await;
        let token = store.current_state_token().await.expect("current token");
        let manifest_bytes = storage
            .get_raw(&paths.manifest_object(token.authority_manifest_id()))
            .await
            .expect("current manifest object");
        let manifest_json: Value = serde_json::from_slice(&manifest_bytes).expect("manifest json");
        let tx_refs = manifest_json["payload"]["tx_refs"]
            .as_array()
            .expect("manifest tx refs")
            .len();
        assert!(
            tx_refs <= 4,
            "manifest suffix {tx_refs} exceeded the checkpoint interval at commit {index}"
        );
        if manifest_json["payload"]["anchor_state"].is_object() {
            boundary_manifest_sizes.push(manifest_bytes.len());
        }
    }

    // The genesis boundary carries no base_state reference, so steady-state
    // size comparison starts at the second boundary.
    let steady_state = &boundary_manifest_sizes[1..];
    let first_boundary = steady_state
        .first()
        .copied()
        .expect("steady-state boundary");
    let last_boundary = steady_state.last().copied().expect("steady-state boundary");
    assert!(
        steady_state.len() >= 3,
        "expected at least three steady-state boundaries, got {steady_state:?}"
    );
    assert!(
        last_boundary <= first_boundary + 64,
        "boundary manifest size must not grow with history: first {first_boundary}, last {last_boundary}"
    );
}

#[tokio::test]
async fn stale_writer_epoch_cannot_publish_and_fenced_state_survives() {
    let (_backend, storage) = storage();
    let store_a = store(storage.clone());

    assert!(
        matches!(
            store(storage.clone()).claim_writer_authority().await,
            Err(CatalogError::Validation { .. })
        ),
        "claiming before genesis must fail closed"
    );

    commit_value(&store_a, b"catalog/default", "genesis").await;

    // Writer A begins before the epoch claim, so its base observes epoch 0.
    let mut stale_txn = store_a
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin stale-epoch transaction");
    stale_txn
        .put(b"catalog/default", Bytes::from_static(b"stale-epoch"))
        .await
        .expect("stage stale write");

    let store_b = store(storage.clone())
        .claim_writer_authority()
        .await
        .expect("claim writer epoch");
    assert_eq!(1, store_b.writer_epoch());

    let error = stale_txn
        .commit()
        .await
        .expect_err("superseded writer must not publish");
    assert!(
        matches!(error, CatalogError::StaleWriterEpoch { .. }),
        "expected typed stale-epoch error, got {error:?}"
    );

    assert!(
        matches!(
            store_a.begin_control_txn(TxnOptions::default()).await,
            Err(CatalogError::StaleWriterEpoch { .. })
        ),
        "superseded writer must fail closed at begin"
    );

    commit_value(&store_b, b"catalog/fenced", "epoch-1").await;
    assert_eq!(
        Some(Bytes::from_static(b"genesis")),
        store_b
            .get(b"catalog/default")
            .await
            .expect("fenced-out write is invisible")
    );
    assert_eq!(
        Some(Bytes::from_static(b"epoch-1")),
        store_b
            .get(b"catalog/fenced")
            .await
            .expect("fenced writer state survives")
    );

    let store_c = store(storage.clone())
        .claim_writer_authority()
        .await
        .expect("claim next writer epoch");
    assert_eq!(2, store_c.writer_epoch(), "epoch claims must be monotone");
    assert!(matches!(
        store_b.begin_control_txn(TxnOptions::default()).await,
        Err(CatalogError::StaleWriterEpoch { .. })
    ));
}

#[tokio::test]
async fn boundary_commit_crash_before_snapshot_registration_is_recoverable() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FailOncePathBackend::new(inner, "/states/"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = ControlMvpStateStore::new(storage.clone(), scope())
        .expect("control MVP store")
        .with_checkpoint_interval(interval(2));

    commit_value(&store, b"catalog/default", "v1").await;

    backend.arm();
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin boundary transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"crashed"))
        .await
        .expect("stage boundary write");
    assert!(
        txn.commit().await.is_err(),
        "injected snapshot crash must interrupt the boundary commit"
    );

    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        store
            .get(b"catalog/default")
            .await
            .expect("crashed boundary commit must not be visible")
    );
    assert_eq!(
        1,
        store
            .current_state_token()
            .await
            .expect("current token")
            .logical_sequence()
    );

    commit_value(&store, b"catalog/default", "v2").await;
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        store
            .get(b"catalog/default")
            .await
            .expect("retried boundary commit is visible")
    );

    // Seed a second live key and a tombstone, so a checkpoint reader that
    // merely returned a coincidentally equal scalar cannot pass.
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin seeding transaction");
    txn.put(b"catalog/sibling", Bytes::from_static(b"sibling-v1"))
        .await
        .expect("stage sibling");
    txn.put(b"catalog/doomed", Bytes::from_static(b"doomed"))
        .await
        .expect("stage doomed key");
    txn.commit().await.expect("commit seeding transaction");
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin tombstone transaction");
    txn.delete(b"catalog/doomed")
        .await
        .expect("stage tombstone");
    txn.commit().await.expect("commit tombstone");
    // One further commit leaves the current manifest without its own anchor,
    // so the explicit checkpoint below must materialize a snapshot rather than
    // reuse one — which is the write the injected crash interrupts.
    commit_value(&store, b"catalog/default", "v3").await;
    assert_eq!(
        Some(Bytes::from_static(b"v3")),
        store
            .get(b"catalog/default")
            .await
            .expect("post-anchor commit reads through the recovered anchor")
    );

    let paths = ControlMvpPaths::new("catalog");
    let inventory_before = backend.list("").await.expect("inventory before").len();
    let pointer_before = storage
        .get_raw(&paths.current_pointer())
        .await
        .expect("pointer before checkpoint");
    backend.arm();
    assert!(
        store
            .checkpoint(CheckpointOptions::default())
            .await
            .is_err(),
        "injected snapshot crash must interrupt the explicit checkpoint"
    );
    assert_eq!(
        pointer_before,
        storage
            .get_raw(&paths.current_pointer())
            .await
            .expect("pointer after interrupted checkpoint"),
        "an interrupted checkpoint must not touch published authority"
    );
    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .expect("checkpoint retry succeeds");
    assert_eq!(
        pointer_before,
        storage
            .get_raw(&paths.current_pointer())
            .await
            .expect("pointer after checkpoint"),
        "checkpointing must not republish authority"
    );
    assert!(
        backend.list("").await.expect("inventory after").len() > inventory_before,
        "the retried checkpoint must materialize its own immutable objects"
    );

    // Identity, not value: the checkpoint token must resolve the exact
    // manifest and snapshot the checkpointed authority names, bound by the
    // checksums recorded in the checkpoint envelope itself.
    let checkpointed_token = store
        .current_state_token()
        .await
        .expect("checkpointed authority token");
    let checkpoint_path = paths.checkpoint_object(checkpoint.checkpoint_id());
    let checkpoint_payload = envelope_payload(&storage, &checkpoint_path).await;
    assert_eq!(
        checkpointed_token.authority_manifest_id(),
        checkpoint_payload["manifest_id"]
            .as_str()
            .expect("checkpoint manifest id")
    );
    assert_eq!(
        checkpointed_token.logical_sequence(),
        checkpoint_payload["logical_sequence"]
            .as_u64()
            .expect("checkpoint sequence")
    );
    let manifest_bytes = storage
        .get_raw(&paths.manifest_object(checkpointed_token.authority_manifest_id()))
        .await
        .expect("checkpointed manifest");
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&manifest_bytes)),
        checkpoint_payload["manifest_checksum_sha256"]
            .as_str()
            .expect("checkpoint manifest checksum"),
        "the checkpoint must be bound to its authority manifest by checksum"
    );
    let snapshot_id = checkpoint_payload["state"]["state_id"]
        .as_str()
        .expect("checkpoint state id")
        .to_string();
    let snapshot_bytes = storage
        .get_raw(&paths.state_object(&snapshot_id))
        .await
        .expect("checkpointed snapshot");
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&snapshot_bytes)),
        checkpoint_payload["state"]["checksum_sha256"]
            .as_str()
            .expect("checkpoint snapshot checksum"),
        "the checkpoint must be bound to its snapshot by checksum"
    );

    // Move current state on, then read the checkpoint: it must serve the
    // checkpointed state for every key, including the tombstoned one.
    commit_value(&store, b"catalog/default", "v4").await;
    commit_value(&store, b"catalog/doomed", "resurrected").await;
    let reader = store
        .read_checkpoint(checkpoint.clone())
        .await
        .expect("checkpoint reader");
    assert_eq!(
        Some(Bytes::from_static(b"v3")),
        reader.get(b"catalog/default").await.expect("checkpoint v3")
    );
    assert_eq!(
        Some(Bytes::from_static(b"sibling-v1")),
        reader
            .get(b"catalog/sibling")
            .await
            .expect("checkpoint sibling")
    );
    assert_eq!(
        None,
        reader
            .get(b"catalog/doomed")
            .await
            .expect("checkpoint tombstone"),
        "a key deleted before the checkpoint must stay absent in it"
    );
    assert_eq!(
        Some(Bytes::from_static(b"resurrected")),
        store
            .get(b"catalog/doomed")
            .await
            .expect("current state moved on")
    );

    // A checkpoint artifact whose recorded manifest checksum no longer matches
    // the manifest it names is refused, even though the envelope itself is
    // internally coherent.
    let checkpoint_bytes = storage
        .get_raw(&checkpoint_path)
        .await
        .expect("checkpoint bytes");
    let corrupted = reseal_envelope(&checkpoint_bytes, |payload| {
        payload.replace(
            checkpoint_payload["manifest_checksum_sha256"]
                .as_str()
                .expect("checkpoint manifest checksum"),
            &"0".repeat(64),
        )
    });
    storage
        .put_raw(&checkpoint_path, corrupted, WritePrecondition::None)
        .await
        .expect("install corrupted checkpoint");
    let error = store
        .read_checkpoint(checkpoint)
        .await
        .err()
        .expect("a checksum-corrupted checkpoint artifact must fail closed");
    assert!(
        matches!(error, CatalogError::InvariantViolation { .. }),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn corrupt_state_snapshot_objects_fail_closed() {
    let (_backend, storage) = storage();
    let store = ControlMvpStateStore::new(storage.clone(), scope())
        .expect("control MVP store")
        .with_checkpoint_interval(interval(1));
    let paths = ControlMvpPaths::new("catalog");

    commit_value(&store, b"catalog/default", "v1").await;
    let anchor_token = store.current_state_token().await.expect("anchor token");
    commit_value(&store, b"catalog/default", "v2").await;

    let anchor_state_id = anchor_token
        .authority_manifest_id()
        .replace("manifest-", "state-");
    let snapshot_path = paths.state_object(&anchor_state_id);
    let original = storage
        .get_raw(&snapshot_path)
        .await
        .expect("anchor snapshot exists");

    rewrite_object_pretty(&storage, &snapshot_path).await;
    let error = store
        .get(b"catalog/default")
        .await
        .expect_err("byte-rewritten snapshot must fail closed");
    assert!(matches!(error, CatalogError::InvariantViolation { .. }));

    let mut tampered: Value = serde_json::from_slice(&original).expect("snapshot json");
    tampered["payload"]["entries"][0]["value"] = Value::Array(vec![Value::from(0_u64)]);
    let payload_bytes =
        serde_json::to_vec(&tampered["payload"]).expect("tampered snapshot payload");
    tampered["checksum_sha256"] = Value::String(hex::encode(sha2::Sha256::digest(&payload_bytes)));
    storage
        .put_raw(
            &snapshot_path,
            Bytes::from(serde_json::to_vec(&tampered).expect("tampered snapshot")),
            WritePrecondition::None,
        )
        .await
        .expect("write tampered snapshot");
    let error = store
        .get(b"catalog/default")
        .await
        .expect_err("value-tampered snapshot must fail closed");
    assert!(matches!(error, CatalogError::InvariantViolation { .. }));

    storage
        .put_raw(&snapshot_path, original, WritePrecondition::None)
        .await
        .expect("restore original snapshot");
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        store
            .get(b"catalog/default")
            .await
            .expect("restored snapshot reads again")
    );
}

#[test]
fn with_writer_epoch_rejects_max_so_claims_cannot_overflow() {
    let (_backend, storage) = storage();
    let error = match store(storage).with_writer_epoch(u64::MAX) {
        Err(error) => error,
        Ok(_) => panic!("u64::MAX cannot leave room for a later authority claim"),
    };
    assert!(
        matches!(error, CatalogError::Validation { .. }),
        "unexpected error: {error:?}"
    );
}

/// Rebuilds a checksum envelope around a mutated payload so the tampered
/// artifact is *internally coherent*: envelope checksum, and any reference
/// digest computed from these bytes, all agree. Substitutions that fail the
/// existing checksum guards prove nothing about authority binding.
fn reseal_envelope(bytes: &[u8], mutate: impl FnOnce(&str) -> String) -> Bytes {
    let text = String::from_utf8(bytes.to_vec()).expect("envelope is utf8");
    let value: Value = serde_json::from_str(&text).expect("envelope json");
    let artifact_type = value["artifact_type"].as_str().expect("artifact type");
    let marker = "\"payload\":";
    let start = text.find(marker).expect("envelope carries a payload") + marker.len();
    let payload = mutate(&text[start..text.len() - 1]);
    let checksum = hex::encode(sha2::Sha256::digest(payload.as_bytes()));
    Bytes::from(format!(
        "{{\"artifact_type\":{},\"checksum_sha256\":\"{checksum}\",\"payload\":{payload}}}",
        serde_json::to_string(artifact_type).expect("artifact type json"),
    ))
}

async fn envelope_payload(storage: &ScopedStorage, path: &str) -> Value {
    let bytes = storage.get_raw(path).await.expect("read envelope");
    serde_json::from_slice::<Value>(&bytes).expect("envelope json")["payload"].clone()
}

/// R5: a checkpoint that names an authority manifest must serve the state
/// that manifest actually asserts. Concurrent losing anchor commits leave
/// valid, same-scope, same-sequence orphan snapshots behind, so sequence
/// agreement alone lets a coherently substituted checkpoint reference select a
/// losing fork.
#[tokio::test]
async fn a_checkpoint_referencing_an_orphan_fork_snapshot_fails_closed() {
    let (backend, storage) = storage();
    let store = ControlMvpStateStore::new(storage.clone(), scope())
        .expect("control MVP store")
        // Anchor every commit so both forks materialize a snapshot object.
        .with_checkpoint_interval(interval(1));
    let paths = ControlMvpPaths::new("catalog");
    commit_value(&store, b"catalog/default", "v1").await;

    // Two transactions race from the same base; both write their manifest and
    // anchor snapshot, and exactly one wins the pointer CAS.
    let mut winner = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin winner");
    let mut loser = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin loser");
    winner
        .put(b"catalog/default", Bytes::from_static(b"winning-fork"))
        .await
        .expect("stage winner");
    loser
        .put(b"catalog/default", Bytes::from_static(b"losing-fork"))
        .await
        .expect("stage loser");
    let winning_token = winner.commit().await.expect("winner publishes");
    let loser_manifest_id = loser.candidate_manifest_id().to_string();
    let error = loser.commit().await.expect_err("loser must lose the CAS");
    assert!(
        matches!(error, CatalogError::CasFailed { .. }),
        "unexpected error: {error:?}"
    );

    // The loser's snapshot survives as a same-scope, same-sequence orphan.
    let orphan_state_id = format!(
        "state-{}",
        loser_manifest_id
            .strip_prefix("manifest-")
            .expect("manifest id prefix")
    );
    let orphan_path = paths.state_object(&orphan_state_id);
    let orphan_bytes = storage
        .get_raw(&orphan_path)
        .await
        .expect("orphan snapshot object exists");
    let orphan_payload = envelope_payload(&storage, &orphan_path).await;
    assert_eq!(
        winning_token.logical_sequence(),
        orphan_payload["logical_sequence"]
            .as_u64()
            .expect("orphan sequence"),
        "the orphan must sit at the same logical sequence as the winner"
    );

    let checkpoint = store
        .checkpoint(CheckpointOptions::default())
        .await
        .expect("checkpoint the winning fork");
    let checkpoint_path = paths.checkpoint_object(checkpoint.checkpoint_id());
    let checkpoint_bytes = storage
        .get_raw(&checkpoint_path)
        .await
        .expect("checkpoint object");
    let winning_state_id = envelope_payload(&storage, &checkpoint_path).await["state"]["state_id"]
        .as_str()
        .expect("winning state id")
        .to_string();
    assert_ne!(winning_state_id, orphan_state_id);

    // Substitute the orphan coherently: the checkpoint still names its real
    // authority manifest, and every checksum in the chain is valid for the
    // bytes it covers.
    let orphan_checksum = hex::encode(sha2::Sha256::digest(&orphan_bytes));
    let tampered = reseal_envelope(&checkpoint_bytes, |payload| {
        payload
            .replace(
                &format!("\"state_id\":\"{winning_state_id}\""),
                &format!("\"state_id\":\"{orphan_state_id}\""),
            )
            .replace(
                &envelope_payload_state_checksum(payload),
                &format!("\"checksum_sha256\":\"{orphan_checksum}\""),
            )
    });
    storage
        .put_raw(&checkpoint_path, tampered, WritePrecondition::None)
        .await
        .expect("install the substituted checkpoint");

    // Sanity: the substitution is coherent, so the guards that predate this
    // check all pass and the fork would otherwise be served.
    let orphan_reader_state = ControlMvpStateStore::new(storage.clone(), scope())
        .expect("store")
        .read_checkpoint(checkpoint.clone())
        .await;
    let error = orphan_reader_state.err().expect(
        "a checkpoint whose snapshot is not the state its authority manifest names must fail closed",
    );
    assert!(
        matches!(&error, CatalogError::InvariantViolation { message }
            if message.contains("not the state named by its authority manifest")),
        "unexpected error: {error:?}"
    );

    // Persisted-reference resolution must fail closed on the same ground: the
    // reference is minted from the substituted bytes, so its own digests match.
    let reference = store
        .persist_checkpoint_reference(&checkpoint, Utc::now() + ChronoDuration::hours(1))
        .await
        .expect("the substitution is coherent enough to mint a reference");
    let error = store
        .resolve_persisted_reference(&reference)
        .await
        .err()
        .expect("resolving a substituted checkpoint reference must fail closed");
    assert!(
        matches!(&error, CatalogError::InvariantViolation { message }
            if message.contains("not the state named by its authority manifest")),
        "unexpected error: {error:?}"
    );

    // Restoring the original checkpoint bytes serves the winning fork again.
    storage
        .put_raw(&checkpoint_path, checkpoint_bytes, WritePrecondition::None)
        .await
        .expect("restore the original checkpoint");
    let reader = store
        .read_checkpoint(checkpoint)
        .await
        .expect("the untampered checkpoint reads");
    assert_eq!(
        Some(Bytes::from_static(b"winning-fork")),
        reader
            .get(b"catalog/default")
            .await
            .expect("checkpoint value")
    );
    assert!(backend.list("").await.expect("inventory").len() > 0);
}

/// Returns the exact `"checksum_sha256":"…"` fragment inside a checkpoint
/// payload's `state` reference, which is the only one that follows `state_id`.
fn envelope_payload_state_checksum(payload: &str) -> String {
    let marker = "\"state\":{";
    let start = payload.find(marker).expect("checkpoint state reference") + marker.len();
    let rest = &payload[start..];
    let checksum_start = rest
        .find("\"checksum_sha256\":\"")
        .expect("state reference checksum");
    let checksum_end = rest[checksum_start + "\"checksum_sha256\":\"".len()..]
        .find('"')
        .expect("state reference checksum end")
        + checksum_start
        + "\"checksum_sha256\":\"".len()
        + 1;
    rest[checksum_start..checksum_end].to_string()
}

/// R4: only the CAS-protected claim advances the published epoch. An
/// arbitrary future epoch supplied from outside must not be able to publish,
/// because publishing would drag the pointer epoch forward and fence out the
/// legitimate holder without ever competing for the claim.
#[tokio::test]
async fn an_unclaimed_future_writer_epoch_cannot_publish_or_advance_the_pointer() {
    let (_backend, storage) = storage();
    let store = store(storage.clone());
    commit_value(&store, b"catalog/default", "v1").await;

    let ahead = store
        .clone()
        .with_writer_epoch(1)
        .expect("epoch 1 is representable");
    let error = match ahead.begin_control_txn(TxnOptions::default()).await {
        Err(error) => error,
        Ok(_) => panic!("an unclaimed current+1 epoch must not begin a publication"),
    };
    assert!(
        matches!(&error, CatalogError::PreconditionFailed { message }
            if message.contains("never claimed")),
        "unexpected error: {error:?}"
    );

    // Far-forward jumps are the same refusal, not a bigger one.
    let jumped = store
        .clone()
        .with_writer_epoch(4096)
        .expect("epoch 4096 is representable");
    let error = match jumped.begin_control_txn(TxnOptions::default()).await {
        Err(error) => error,
        Ok(_) => panic!("a large forward jump must not publish either"),
    };
    assert!(
        matches!(&error, CatalogError::PreconditionFailed { message }
            if message.contains("never claimed")),
        "unexpected error: {error:?}"
    );

    // The pointer epoch is untouched, so the claim protocol still works and a
    // claimed epoch publishes normally.
    let claimed = store
        .clone()
        .claim_writer_authority()
        .await
        .expect("claim the next epoch");
    assert_eq!(1, claimed.writer_epoch());
    commit_value(&claimed, b"catalog/default", "v2").await;
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        store
            .get(b"catalog/default")
            .await
            .expect("claimed publish")
    );

    // The superseded holder is fenced out with the stale-epoch error, and a
    // writer that cooperatively adopts the published epoch keeps working.
    let error = match store.clone().begin_control_txn(TxnOptions::default()).await {
        Err(error) => error,
        Ok(_) => panic!("the superseded epoch 0 holder must fail closed"),
    };
    assert!(
        matches!(error, CatalogError::StaleWriterEpoch { .. }),
        "unexpected error: {error:?}"
    );
    let cooperative = store
        .clone()
        .at_current_writer_epoch()
        .await
        .expect("adopt the published epoch");
    commit_value(&cooperative, b"catalog/default", "v3").await;
}

/// Rewrites the published pointer's writer epoch in place, leaving every other
/// field (and therefore the manifest it selects) untouched. This is the only
/// way to reach the representable ceiling of the claim state machine without
/// performing 2^64 claims.
async fn force_pointer_writer_epoch(storage: &ScopedStorage, epoch: u64) {
    let path = ControlMvpPaths::new("catalog").current_pointer();
    let mut pointer: Value = serde_json::from_slice(
        &storage
            .get_raw(&path)
            .await
            .expect("published pointer exists"),
    )
    .expect("pointer json");
    pointer["writer_epoch"] = Value::from(epoch);
    storage
        .put_raw(
            &path,
            Bytes::from(serde_json::to_vec(&pointer).expect("pointer bytes")),
            WritePrecondition::None,
        )
        .await
        .expect("force pointer writer epoch");
}

async fn published_writer_epoch(storage: &ScopedStorage) -> u64 {
    let path = ControlMvpPaths::new("catalog").current_pointer();
    let pointer: Value =
        serde_json::from_slice(&storage.get_raw(&path).await.expect("published pointer"))
            .expect("pointer json");
    pointer["writer_epoch"].as_u64().expect("writer epoch")
}

/// R4: the epoch ceiling has to be exercised through the real claim state
/// machine, not through the setter alone. A value-only assertion cannot
/// distinguish "the next claim is refused before publication" from "the next
/// claim overflows, wraps, or panics" — and those are different safety
/// contracts. This pins the intended one end to end.
#[tokio::test]
async fn writer_epoch_claims_at_the_representable_ceiling_never_wrap_or_panic() {
    let (_backend, storage) = storage();
    let store = store(storage.clone());
    commit_value(&store, b"catalog/default", "v1").await;

    // One claim below the ceiling: the claim is legitimate and publishes.
    force_pointer_writer_epoch(&storage, u64::MAX - 2).await;
    let claimed = store
        .clone()
        .claim_writer_authority()
        .await
        .expect("the last legitimate claim must succeed");
    assert_eq!(u64::MAX - 1, claimed.writer_epoch());
    assert_eq!(u64::MAX - 1, published_writer_epoch(&storage).await);
    commit_value(&claimed, b"catalog/default", "ceiling").await;
    assert_eq!(u64::MAX - 1, published_writer_epoch(&storage).await);

    // The follow-on claim would publish u64::MAX. It is refused *before*
    // publication, with a typed error, and nothing about the pointer moves.
    let error = match store.clone().claim_writer_authority().await {
        Err(error) => error,
        Ok(_) => panic!("a claim that would publish u64::MAX must be refused"),
    };
    assert!(
        matches!(&error, CatalogError::Validation { message }
            if message.contains("rejected rather than saturated")),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        u64::MAX - 1,
        published_writer_epoch(&storage).await,
        "a refused claim must not advance, wrap, or regress the published epoch"
    );

    // The domain is not wedged for writes: the published epoch still
    // publishes, and cooperative writers still adopt it.
    let cooperative = store
        .clone()
        .at_current_writer_epoch()
        .await
        .expect("adopt the ceiling epoch");
    assert_eq!(u64::MAX - 1, cooperative.writer_epoch());
    commit_value(&cooperative, b"catalog/default", "still-usable").await;
    assert_eq!(
        Some(Bytes::from_static(b"still-usable")),
        store
            .get(b"catalog/default")
            .await
            .expect("the ceiling epoch still publishes")
    );

    // u64::MAX can never be pinned, so no route reaches a published u64::MAX.
    assert!(
        store.clone().with_writer_epoch(u64::MAX).is_err(),
        "u64::MAX must be unreachable as a publication epoch"
    );

    // Defence in depth: even a pointer forced to u64::MAX — which no claim can
    // produce — fails closed with a typed error rather than overflowing.
    force_pointer_writer_epoch(&storage, u64::MAX).await;
    let error = match store.clone().claim_writer_authority().await {
        Err(error) => error,
        Ok(_) => panic!("claiming past u64::MAX must fail closed"),
    };
    assert!(
        matches!(&error, CatalogError::Validation { message } if message.contains("overflow")),
        "unexpected error: {error:?}"
    );
    assert_eq!(
        u64::MAX,
        published_writer_epoch(&storage).await,
        "a failed claim must never wrap the published epoch to zero"
    );
}

/// R4: `u64::MAX` is refused everywhere instead of being saturated, and every
/// epoch that a caller *can* successfully supply still leaves room for the
/// next claim — the property the old saturation quietly destroyed.
#[tokio::test]
async fn public_epoch_inputs_never_wedge_the_claim_protocol() {
    let (_backend, storage) = storage();
    let store = store(storage.clone());
    commit_value(&store, b"catalog/default", "v1").await;

    for rejected in [u64::MAX, u64::MAX] {
        let error = match store.clone().with_writer_epoch(rejected) {
            Err(error) => error,
            Ok(_) => panic!("{rejected} must be refused, not saturated"),
        };
        assert!(
            matches!(&error, CatalogError::Validation { message }
                if message.contains("rejected rather than saturated")),
            "unexpected error: {error:?}"
        );
    }

    // Every accepted public epoch input leaves the claim chain intact: after
    // each one, another claim is still possible.
    let mut current = store.clone();
    for expected in 1..=5_u64 {
        for candidate in [0_u64, expected, expected + 1, u64::MAX - 1] {
            // Accepting the value as a *pin* is not accepting it as authority;
            // only the published epoch may publish.
            store
                .clone()
                .with_writer_epoch(candidate)
                .expect("any epoch below u64::MAX is a representable pin");
        }
        current = current
            .claim_writer_authority()
            .await
            .expect("a further claim remains possible after every accepted input");
        assert_eq!(expected, current.writer_epoch());
        commit_value(&current, b"catalog/default", "vn").await;
    }
}

#[tokio::test]
async fn duplicate_projection_outbox_ids_fail_at_stage_time_and_domain_stays_trimmable() {
    let (_backend, storage) = storage();
    let store = store(storage);

    // Duplicate staging within one transaction fails with the typed error.
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin transaction");
    txn.stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
        "record-r",
        Bytes::from_static(b"payload-a"),
    ))
    .expect("first staging");
    let error = txn
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "record-r",
            Bytes::from_static(b"payload-dup"),
        ))
        .expect_err("duplicate staging within a transaction must fail");
    assert!(
        matches!(error, CatalogError::AlreadyExists { .. }),
        "unexpected error: {error:?}"
    );
    let winning_token = txn.commit().await.expect("commit winner");

    // Concurrent duplicate staging: both transactions began before either
    // committed; exactly one wins the pointer CAS, and the loser's retry
    // revalidates against the winning state and gets the typed error.
    let mut loser = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin loser");
    loser
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "record-s",
            Bytes::from_static(b"loser"),
        ))
        .expect("loser stages a fresh id against its base");
    let mut concurrent_winner = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin concurrent winner");
    concurrent_winner
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "record-s",
            Bytes::from_static(b"winner"),
        ))
        .expect("winner stages the same id concurrently");
    concurrent_winner
        .commit()
        .await
        .expect("concurrent winner commits");
    assert!(matches!(
        loser.commit().await,
        Err(CatalogError::CasFailed { .. })
    ));
    let mut retry = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin retry");
    let error = retry
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "record-s",
            Bytes::from_static(b"retry"),
        ))
        .expect_err("retry against the winning state must reject the duplicate id");
    assert!(
        matches!(&error, CatalogError::AlreadyExists { entity, name }
            if entity == "projection outbox record" && name == "record-s"),
        "unexpected error: {error:?}"
    );

    // Exactly one record per id is retained, so acknowledgement and trimming
    // stay functional: the domain cannot be wedged untrimmable.
    let outbox = store
        .current_projection_outbox()
        .await
        .expect("current outbox");
    assert_eq!(
        vec!["record-r".to_string(), "record-s".to_string()],
        outbox
            .iter()
            .map(|record| record.record_id().to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(1, winning_token.logical_sequence());
    let mut trim_txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin trim");
    trim_txn
        .trim_projection_outbox(vec![
            ControlMvpOutboxTrimTarget::new("record-r", 1),
            ControlMvpOutboxTrimTarget::new("record-s", 2),
        ])
        .expect("trim stays functional");
    trim_txn.commit().await.expect("commit trim");
    assert!(
        store
            .current_projection_outbox()
            .await
            .expect("outbox after trim")
            .is_empty()
    );

    // A trimmed id is re-stageable as a fresh record.
    let mut restage = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin restage");
    restage
        .stage_projection_outbox(ControlMvpProjectionOutboxRecord::new(
            "record-r",
            Bytes::from_static(b"payload-b"),
        ))
        .expect("trimmed id is stageable again");
    restage.commit().await.expect("commit restage");
}

#[tokio::test]
async fn restore_inspection_stays_visible_across_anchor_boundaries() {
    let (_backend, storage) = storage();
    let store = ControlMvpStateStore::new(storage, scope())
        .expect("control MVP store")
        .with_checkpoint_interval(interval(2));
    let source = retained_v1_and_current_v2(&store).await;
    let adapter = ControlMvpRestoreParticipant::new(store.clone());
    let plan = adapter
        .plan_restore(
            &source,
            &RestoreAttemptIdentity::new("rst_00000000000000000000000010", 1, "catalog")
                .expect("identity"),
            Utc::now(),
        )
        .await
        .expect("plan");
    let applied = adapter
        .apply_restore(&plan, Utc::now())
        .await
        .expect("apply");
    let RestoreParticipantInspection::Visible { token, .. } = applied else {
        panic!("restore must become visible");
    };
    assert_eq!(3, token.logical_sequence());

    // Commit past at least two replay-anchor boundaries so the bounded
    // transaction suffix no longer covers the restore transaction.
    for index in 0..6 {
        commit_value(&store, b"catalog/later", &format!("v{index}")).await;
    }

    let inspection = adapter
        .inspect_restore(&plan)
        .await
        .expect("anchor-crossing inspect");
    let RestoreParticipantInspection::Visible { token, evidence } = inspection else {
        panic!(
            "an applied restore must remain Visible across anchor boundaries, got Superseded/Ready"
        );
    };
    assert_eq!(3, token.logical_sequence());
    assert_eq!(3, evidence.logical_sequence());
}

#[tokio::test]
async fn genuinely_superseded_restore_stays_superseded_across_anchor_boundaries() {
    let (_backend, storage) = storage();
    let store = ControlMvpStateStore::new(storage, scope())
        .expect("control MVP store")
        .with_checkpoint_interval(interval(2));
    let source = retained_v1_and_current_v2(&store).await;
    let adapter = ControlMvpRestoreParticipant::new(store.clone());
    let plan = adapter
        .plan_restore(
            &source,
            &RestoreAttemptIdentity::new("rst_00000000000000000000000011", 1, "catalog")
                .expect("identity"),
            Utc::now(),
        )
        .await
        .expect("plan");

    // A foreign writer wins the planned sequence, then history keeps moving
    // across anchor boundaries.
    let mut foreign = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("foreign transaction");
    foreign
        .put(b"catalog/foreign", Bytes::from_static(b"winner"))
        .await
        .expect("foreign write");
    foreign.commit().await.expect("foreign commit");
    for index in 0..6 {
        commit_value(&store, b"catalog/later", &format!("v{index}")).await;
    }

    assert!(matches!(
        adapter
            .inspect_restore(&plan)
            .await
            .expect("anchor-crossing superseded inspect"),
        RestoreParticipantInspection::Superseded
    ));
    assert!(matches!(
        adapter
            .apply_restore(&plan, Utc::now())
            .await
            .expect("superseded apply"),
        RestoreParticipantInspection::Superseded
    ));
}

#[tokio::test]
async fn boundary_commit_crash_after_anchor_snapshot_before_pointer_cas_is_recoverable() {
    let inner: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backend = Arc::new(FailOncePathBackend::new(inner, "/current.pointer.json"));
    let storage = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let store = ControlMvpStateStore::new(storage.clone(), scope())
        .expect("control MVP store")
        .with_checkpoint_interval(interval(2));
    let paths = ControlMvpPaths::new("catalog");

    commit_value(&store, b"catalog/default", "v1").await;

    // Torn boundary state: the transaction object, the anchor snapshot, and
    // the manifest ARE persisted, but the pointer CAS fails.
    backend.arm();
    let mut txn = store
        .begin_control_txn(TxnOptions::default())
        .await
        .expect("begin boundary transaction");
    txn.put(b"catalog/default", Bytes::from_static(b"torn"))
        .await
        .expect("stage boundary write");
    let torn_manifest_id = txn.candidate_manifest_id().to_string();
    assert!(
        txn.commit().await.is_err(),
        "injected pointer crash must interrupt the boundary commit"
    );

    let torn_state_id = torn_manifest_id.replace("manifest-", "state-");
    storage
        .get_raw(&paths.state_object(&torn_state_id))
        .await
        .expect("the torn boundary's anchor snapshot IS persisted");
    storage
        .get_raw(&paths.manifest_object(&torn_manifest_id))
        .await
        .expect("the torn boundary's manifest IS persisted");
    assert_eq!(
        Some(Bytes::from_static(b"v1")),
        store
            .get(b"catalog/default")
            .await
            .expect("torn boundary commit is not visible")
    );
    assert_eq!(
        1,
        store
            .current_state_token()
            .await
            .expect("current token")
            .logical_sequence()
    );

    // The writer retries: the retried boundary anchors its own snapshot and
    // publishes; the orphaned snapshot is inert.
    commit_value(&store, b"catalog/default", "v2").await;
    assert_eq!(
        Some(Bytes::from_static(b"v2")),
        store
            .get(b"catalog/default")
            .await
            .expect("retried boundary commit is visible without checksum failures")
    );
    let recovered_token = store.current_state_token().await.expect("recovered token");
    assert_eq!(2, recovered_token.logical_sequence());
    assert_ne!(torn_manifest_id, recovered_token.authority_manifest_id());
    let manifest_bytes = storage
        .get_raw(&paths.manifest_object(recovered_token.authority_manifest_id()))
        .await
        .expect("recovered manifest object");
    let manifest_json: Value = serde_json::from_slice(&manifest_bytes).expect("manifest json");
    assert_eq!(
        Value::String(
            recovered_token
                .authority_manifest_id()
                .replace("manifest-", "state-")
        ),
        manifest_json["payload"]["anchor_state"]["state_id"],
        "the recovered boundary must anchor its own snapshot, not the orphan"
    );
    storage
        .get_raw(&paths.state_object(&torn_state_id))
        .await
        .expect("the orphaned snapshot remains physically present but unreferenced");

    // Replay stays bounded and checksum-clean through further boundaries.
    for index in 3..8 {
        commit_value(&store, b"catalog/default", &format!("v{index}")).await;
    }
    assert_eq!(
        Some(Bytes::from_static(b"v7")),
        store
            .get(b"catalog/default")
            .await
            .expect("post-recovery reads replay through the recovered anchors")
    );
    let final_token = store.current_state_token().await.expect("final token");
    let final_manifest = storage
        .get_raw(&paths.manifest_object(final_token.authority_manifest_id()))
        .await
        .expect("final manifest object");
    let final_json: Value = serde_json::from_slice(&final_manifest).expect("final manifest json");
    assert!(
        final_json["payload"]["tx_refs"]
            .as_array()
            .is_some_and(|refs| refs.len() <= 2),
        "replay suffix must stay bounded by the checkpoint interval"
    );
}
