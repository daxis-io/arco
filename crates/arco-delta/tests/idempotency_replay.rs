//! Regression tests for idempotency replay around transient write failures.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashSet;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_core::{Error as CoreError, Result as CoreResult, ScopedStorage};
use arco_delta::{
    CommitDeltaRequest, DeltaCommitCoordinator, DeltaCoordinatorState, InflightCommit,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

const TENANT: &str = "acme";
const WORKSPACE: &str = "analytics";

#[derive(Debug, Default)]
struct FailOnceBackend {
    inner: MemoryBackend,
    fail_once_put_paths: Arc<Mutex<HashSet<String>>>,
}

impl FailOnceBackend {
    fn new() -> Self {
        Self::default()
    }

    fn fail_next_put_on_exact_path(&self, path: &str) {
        self.fail_once_put_paths
            .lock()
            .expect("lock")
            .insert(path.to_string());
    }

    fn should_fail_put(&self, path: &str) -> bool {
        self.fail_once_put_paths.lock().expect("lock").remove(path)
    }
}

#[async_trait]
impl StorageBackend for FailOnceBackend {
    async fn get(&self, path: &str) -> CoreResult<Bytes> {
        self.inner.get(path).await
    }

    async fn get_range(&self, path: &str, range: Range<u64>) -> CoreResult<Bytes> {
        self.inner.get_range(path, range).await
    }

    async fn put(
        &self,
        path: &str,
        data: Bytes,
        precondition: WritePrecondition,
    ) -> CoreResult<WriteResult> {
        if self.should_fail_put(path) {
            return Err(CoreError::storage(format!(
                "injected write failure: {path}"
            )));
        }
        self.inner.put(path, data, precondition).await
    }

    async fn delete(&self, path: &str) -> CoreResult<()> {
        self.inner.delete(path).await
    }

    async fn list(&self, prefix: &str) -> CoreResult<Vec<ObjectMeta>> {
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> CoreResult<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(&self, path: &str, expiry: Duration) -> CoreResult<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn scoped_path(path: &str) -> String {
    format!("tenant={TENANT}/workspace={WORKSPACE}/{path}")
}

fn idempotency_record_relative_path(table_id: Uuid, idempotency_key: &str) -> String {
    let hash = sha256_hex(idempotency_key.as_bytes());
    let prefix = hash.get(0..2).unwrap_or("00");
    format!("delta/idempotency/{table_id}/{prefix}/{hash}.json")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[tokio::test]
async fn commit_retry_replays_after_transient_idempotency_write_failure() {
    let backend = Arc::new(FailOnceBackend::new());
    let storage = ScopedStorage::new(backend.clone(), TENANT, WORKSPACE).expect("scoped storage");
    let table_id = Uuid::now_v7();
    let coordinator = DeltaCommitCoordinator::new(storage.clone(), table_id);

    let staged = coordinator
        .stage_commit_payload(Bytes::from_static(b"{\"commitInfo\":{\"ts\":1}}\n"))
        .await
        .expect("stage payload");

    let idempotency_key = Uuid::now_v7().to_string();
    let request = CommitDeltaRequest {
        read_version: -1,
        staged_path: staged.staged_path.clone(),
        staged_version: staged.staged_version.clone(),
        idempotency_key: idempotency_key.clone(),
    };

    let record_path = idempotency_record_relative_path(table_id, &idempotency_key);
    backend.fail_next_put_on_exact_path(&scoped_path(&record_path));

    let first = coordinator.commit(request.clone(), Utc::now()).await;
    assert!(
        first.is_err(),
        "first attempt should fail on injected write"
    );

    let replay = coordinator
        .commit(request, Utc::now())
        .await
        .expect("retry should replay committed response");
    assert_eq!(replay.version, 0);
    assert_eq!(
        replay.delta_log_path,
        format!("tables/{table_id}/_delta_log/00000000000000000000.json")
    );

    assert!(
        storage
            .head_raw(&record_path)
            .await
            .expect("head idempotency record")
            .is_some()
    );
}

#[tokio::test]
async fn recover_retry_replays_after_transient_idempotency_write_failure() {
    let backend = Arc::new(FailOnceBackend::new());
    let storage = ScopedStorage::new(backend.clone(), TENANT, WORKSPACE).expect("scoped storage");
    let table_id = Uuid::now_v7();
    let coordinator = DeltaCommitCoordinator::new(storage.clone(), table_id);

    let staged = coordinator
        .stage_commit_payload(Bytes::from_static(b"{\"commitInfo\":{\"ts\":2}}\n"))
        .await
        .expect("stage payload");

    let idempotency_key = Uuid::now_v7().to_string();
    let now_ms = Utc::now().timestamp_millis();
    let seed_state = DeltaCoordinatorState {
        latest_version: -1,
        inflight: Some(InflightCommit {
            commit_id: idempotency_key.clone(),
            read_version: Some(-1),
            version: 0,
            staged_path: staged.staged_path.clone(),
            staged_version: staged.staged_version.clone(),
            started_at_ms: now_ms - 30_000,
            expires_at_ms: now_ms - 10_000,
        }),
    };
    storage
        .put_raw(
            &format!("delta/coordinator/{table_id}.json"),
            Bytes::from(serde_json::to_vec(&seed_state).expect("serialize seed state")),
            WritePrecondition::DoesNotExist,
        )
        .await
        .expect("seed coordinator state");

    let request = CommitDeltaRequest {
        read_version: -1,
        staged_path: staged.staged_path.clone(),
        staged_version: staged.staged_version.clone(),
        idempotency_key: idempotency_key.clone(),
    };

    let record_path = idempotency_record_relative_path(table_id, &idempotency_key);
    backend.fail_next_put_on_exact_path(&scoped_path(&record_path));

    let first = coordinator.commit(request.clone(), Utc::now()).await;
    assert!(
        first.is_err(),
        "first recovery attempt should fail on injected idempotency write"
    );

    let replay = coordinator
        .commit(request, Utc::now())
        .await
        .expect("retry should replay recovered commit");
    assert_eq!(replay.version, 0);
    assert_eq!(
        replay.delta_log_path,
        format!("tables/{table_id}/_delta_log/00000000000000000000.json")
    );

    let state_bytes = storage
        .get_raw(&format!("delta/coordinator/{table_id}.json"))
        .await
        .expect("read coordinator state");
    let state: DeltaCoordinatorState =
        serde_json::from_slice(&state_bytes).expect("deserialize coordinator state");
    assert_eq!(state.latest_version, 0);
    assert!(state.inflight.is_none());
}
