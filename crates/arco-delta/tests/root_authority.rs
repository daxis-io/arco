//! Root binding for managed Delta commit coordination.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use arco_core::storage::{MemoryBackend, WritePrecondition};
use arco_core::{DeltaPaths, ScopedStorage};
use arco_delta::{CommitDeltaRequest, DeltaCommitCoordinator, DeltaError};
use bytes::Bytes;
use chrono::Utc;
use serde_json::{Value, json};
use uuid::Uuid;

const ROOT: &str = "warehouse/analytics/sales/orders";

fn fixture() -> (ScopedStorage, DeltaPaths, DeltaPaths) {
    let storage = ScopedStorage::new(Arc::new(MemoryBackend::new()), "tenant1", "workspace1")
        .expect("scoped storage");
    let table_id = Uuid::now_v7();
    let registered = DeltaPaths::new(table_id, ROOT).expect("registered root");
    (storage, registered, DeltaPaths::legacy(table_id))
}

fn payload() -> Bytes {
    Bytes::from_static(b"{\"commitInfo\":{}}\n")
}

async fn seed_legacy_history(
    storage: &ScopedStorage,
    paths: &DeltaPaths,
    latest_version: i64,
    logs: &[(&DeltaPaths, i64)],
) -> Bytes {
    let state =
        Bytes::from(json!({"latest_version": latest_version, "inflight": null}).to_string());
    storage
        .put_raw(
            &paths.coordinator_state(),
            state.clone(),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("legacy coordinator");
    for (log_paths, version) in logs {
        storage
            .put_raw(
                &log_paths.delta_log_json(*version).expect("log path"),
                payload(),
                WritePrecondition::DoesNotExist,
            )
            .await
            .expect("legacy log");
    }
    state
}

#[tokio::test]
async fn first_stage_binds_the_root_before_an_alternate_stage() {
    let (storage, registered, legacy) = fixture();
    let coordinator = DeltaCommitCoordinator::with_paths(storage.clone(), registered.clone());
    let alternate = DeltaCommitCoordinator::with_paths(storage.clone(), legacy);
    let staged = coordinator
        .stage_commit_payload(payload())
        .await
        .expect("stage at registered root");

    assert!(matches!(
        alternate.stage_commit_payload(payload()).await,
        Err(DeltaError::Conflict { message }) if message == "delta_coordinator_root_mismatch"
    ));
    let staged_paths = storage
        .list(&format!("delta/staging/{}/", registered.table_id()))
        .await
        .expect("list staged payloads");
    assert_eq!(staged_paths.len(), 1);
    assert_eq!(staged_paths[0].as_str(), staged.staged_path);
}

#[tokio::test]
async fn replay_requires_the_bound_commit_root() {
    let (storage, registered, legacy) = fixture();
    let coordinator = DeltaCommitCoordinator::with_paths(storage.clone(), registered);
    let alternate = DeltaCommitCoordinator::with_paths(storage, legacy);
    let staged = coordinator
        .stage_commit_payload(payload())
        .await
        .expect("stage payload");
    let request = CommitDeltaRequest {
        read_version: -1,
        staged_path: staged.staged_path,
        staged_version: staged.staged_version,
        idempotency_key: Uuid::now_v7().to_string(),
    };
    coordinator
        .commit(request.clone(), Utc::now())
        .await
        .expect("commit at registered root");

    assert!(matches!(
        alternate.replay_committed(&request).await,
        Err(DeltaError::Conflict { message }) if message == "delta_coordinator_root_mismatch"
    ));
}

#[tokio::test]
async fn owner_replacement_rejects_an_alternate_root_before_staging() {
    let (storage, registered, legacy) = fixture();
    let owner = DeltaCommitCoordinator::with_paths(storage.clone(), registered.clone());
    let alternate = DeltaCommitCoordinator::with_paths(storage.clone(), legacy);
    let key = Uuid::now_v7().to_string();
    owner
        .commit_owner_replacement(&key, -1, payload(), Utc::now())
        .await
        .expect("publish at the registered root");

    assert!(matches!(
        alternate.commit_owner_replacement(&Uuid::now_v7().to_string(), 0, payload(), Utc::now()).await,
        Err(DeltaError::Conflict { message }) if message == "delta_coordinator_root_mismatch"
    ));
    let staged = storage
        .list(&format!("delta/staging/{}/", registered.table_id()))
        .await
        .expect("list staged payloads");
    assert_eq!(
        staged.len(),
        1,
        "the alternate root must not stage a payload"
    );
    assert!(matches!(
        alternate.read_owner_replacement(&key).await,
        Err(DeltaError::Conflict { message }) if message == "delta_coordinator_root_mismatch"
    ));
}

#[tokio::test]
async fn existing_owner_replacement_can_recover_at_its_legacy_root() {
    let (storage, registered, _) = fixture();
    let owner = DeltaCommitCoordinator::with_paths(storage.clone(), registered.clone());
    let key = Uuid::now_v7().to_string();
    owner
        .commit_owner_replacement(&key, -1, payload(), Utc::now())
        .await
        .expect("publish at the registered root");
    let path = registered.coordinator_state();
    let mut legacy: Value = serde_json::from_slice(&storage.get_raw(&path).await.expect("state"))
        .expect("decode state");
    legacy
        .as_object_mut()
        .expect("state object")
        .remove("table_root");
    let legacy = Bytes::from(legacy.to_string());
    storage
        .put_raw(&path, legacy.clone(), WritePrecondition::None)
        .await
        .expect("retained unbound history");

    assert_eq!(
        owner
            .read_owner_replacement(&key)
            .await
            .expect("read legacy receipt"),
        Some((0, payload()))
    );
    assert_eq!(storage.get_raw(&path).await.expect("unbound state"), legacy);
    assert_eq!(
        owner
            .recover_owner_replacement(&key)
            .await
            .expect("recover legacy receipt"),
        Some((0, payload()))
    );
    let bound: Value = serde_json::from_slice(&storage.get_raw(&path).await.expect("state"))
        .expect("decode state");
    assert_eq!(bound["table_root"], ROOT);
}

#[tokio::test]
async fn complete_legacy_history_at_the_registered_root_can_be_bound() {
    let (storage, registered, _) = fixture();
    seed_legacy_history(
        &storage,
        &registered,
        1,
        &[(&registered, 0), (&registered, 1)],
    )
    .await;
    let coordinator = DeltaCommitCoordinator::with_paths(storage.clone(), registered.clone());
    coordinator
        .stage_commit_payload(payload())
        .await
        .expect("bind a contiguous history");

    let state = storage
        .get_raw(&registered.coordinator_state())
        .await
        .expect("coordinator state");
    let decoded: Value = serde_json::from_slice(&state).expect("decode coordinator");
    assert_eq!(decoded["table_root"], ROOT);
    assert_eq!(decoded["latest_version"], 1);
}

#[tokio::test]
async fn latest_log_at_the_registered_root_does_not_hide_an_earlier_split() {
    let (storage, registered, legacy) = fixture();
    let before =
        seed_legacy_history(&storage, &registered, 1, &[(&legacy, 0), (&registered, 1)]).await;
    let coordinator = DeltaCommitCoordinator::with_paths(storage.clone(), registered.clone());

    assert!(matches!(
        coordinator.stage_commit_payload(payload()).await,
        Err(DeltaError::Conflict { message }) if message == "delta_coordinator_root_migration_required"
    ));
    assert_eq!(
        storage
            .get_raw(&registered.coordinator_state())
            .await
            .expect("coordinator state"),
        before
    );
    let staged = storage
        .list(&format!("delta/staging/{}/", registered.table_id()))
        .await
        .expect("list staged payloads");
    assert!(staged.is_empty());
}
