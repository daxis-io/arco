//! DR regression tests for orchestration rebuild from ledger manifests.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Duration, Utc};

use arco_core::ScopedStorage;
use arco_core::storage::{
    MemoryBackend, ObjectMeta, StorageBackend, WritePrecondition, WriteResult,
};
use arco_flow::error::{Error as FlowError, Result};
use arco_flow::orchestration::compactor::{
    LedgerRebuildManifest, MicroCompactor, OrchestrationManifest, OrchestrationManifestPointer,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TaskOutcome, TriggerInfo,
};

const TENANT: &str = "tenant";
const WORKSPACE: &str = "workspace";
const POINTER_PATH: &str = "state/orchestration/manifest.pointer.json";
const LEGACY_MANIFEST_PATH: &str = "state/orchestration/manifest.json";

#[derive(Debug)]
struct ListTrackingBackend {
    inner: MemoryBackend,
    list_calls: AtomicUsize,
}

impl ListTrackingBackend {
    fn new() -> Self {
        Self {
            inner: MemoryBackend::new(),
            list_calls: AtomicUsize::new(0),
        }
    }

    fn list_calls(&self) -> usize {
        self.list_calls.load(Ordering::Relaxed)
    }

    fn reset_list_calls(&self) {
        self.list_calls.store(0, Ordering::Relaxed);
    }
}

#[async_trait]
impl StorageBackend for ListTrackingBackend {
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
        if prefix.contains("ledger/") {
            self.list_calls.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.list(prefix).await
    }

    async fn head(&self, path: &str) -> arco_core::Result<Option<ObjectMeta>> {
        self.inner.head(path).await
    }

    async fn signed_url(
        &self,
        path: &str,
        expiry: std::time::Duration,
    ) -> arco_core::Result<String> {
        self.inner.signed_url(path, expiry).await
    }
}

fn run_triggered(event_id: &str, run_id: &str, plan_id: &str) -> OrchestrationEvent {
    let mut event = OrchestrationEvent::new_with_timestamp(
        TENANT,
        WORKSPACE,
        OrchestrationEventData::RunTriggered {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            trigger: TriggerInfo::Manual {
                user_id: "dr-test".to_string(),
            },
            root_assets: Vec::new(),
            run_key: None,
            labels: std::collections::HashMap::new(),
            code_version: None,
        },
        Utc::now(),
    );
    event.event_id = event_id.to_string();
    event
}

fn plan_created(event_id: &str, run_id: &str, plan_id: &str) -> OrchestrationEvent {
    let mut event = OrchestrationEvent::new_with_timestamp(
        TENANT,
        WORKSPACE,
        OrchestrationEventData::PlanCreated {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            tasks: vec![TaskDef {
                key: "extract".to_string(),
                depends_on: Vec::new(),
                asset_key: None,
                partition_key: None,
                max_attempts: 3,
                heartbeat_timeout_sec: 300,
            }],
        },
        Utc::now(),
    );
    event.event_id = event_id.to_string();
    event
}

fn task_finished(event_id: &str, run_id: &str, attempt_id: &str) -> OrchestrationEvent {
    let mut event = OrchestrationEvent::new_with_timestamp(
        TENANT,
        WORKSPACE,
        OrchestrationEventData::TaskFinished {
            run_id: run_id.to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            attempt_id: attempt_id.to_string(),
            worker_id: "worker-dr".to_string(),
            outcome: TaskOutcome::Succeeded,
            materialization_id: None,
            error_message: None,
            output: None,
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
            asset_key: None,
            partition_key: None,
            code_version: None,
        },
        Utc::now(),
    );
    event.event_id = event_id.to_string();
    event
}

async fn write_event(storage: &ScopedStorage, event: &OrchestrationEvent) -> Result<String> {
    let path = format!("ledger/orchestration/2026-02-21/{}.json", event.event_id);
    storage
        .put_raw(
            &path,
            Bytes::from(serde_json::to_vec(event).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    Ok(path)
}

#[tokio::test]
async fn rebuild_from_ledger_manifest_reproduces_equivalent_projection_state() -> Result<()> {
    let baseline_backend = Arc::new(MemoryBackend::new());
    let baseline_storage = ScopedStorage::new(baseline_backend, TENANT, WORKSPACE)?;
    let baseline_compactor = MicroCompactor::new(baseline_storage.clone());

    let rebuild_backend = Arc::new(MemoryBackend::new());
    let rebuild_storage = ScopedStorage::new(rebuild_backend, TENANT, WORKSPACE)?;
    let rebuild_compactor = MicroCompactor::new(rebuild_storage.clone());

    let run_id = "run-dr-equivalence";
    let plan_id = "plan-dr-equivalence";
    let attempt_id = "01J1DRREBUILDATTEMPT000000";

    let events = vec![
        run_triggered("01J1DRREBUILD00000000000001", run_id, plan_id),
        plan_created("01J1DRREBUILD00000000000002", run_id, plan_id),
        task_finished("01J1DRREBUILD00000000000003", run_id, attempt_id),
    ];

    let mut paths = Vec::new();
    for event in &events {
        let baseline_path = write_event(&baseline_storage, event).await?;
        let rebuild_path = write_event(&rebuild_storage, event).await?;
        assert_eq!(baseline_path, rebuild_path);
        paths.push(baseline_path);
    }

    baseline_compactor.compact_events(paths.clone()).await?;

    let rebuild_manifest = LedgerRebuildManifest {
        event_paths: vec![
            paths[2].clone(),
            paths[0].clone(),
            paths[1].clone(),
            paths[2].clone(),
        ],
    };
    rebuild_compactor
        .rebuild_from_ledger_manifest(rebuild_manifest, None)
        .await?;

    let (baseline_manifest, baseline_state) = baseline_compactor.load_state().await?;
    let (rebuild_manifest, rebuild_state) = rebuild_compactor.load_state().await?;

    assert_eq!(baseline_state.runs, rebuild_state.runs);
    assert_eq!(baseline_state.tasks, rebuild_state.tasks);
    assert_eq!(
        baseline_state.dep_satisfaction,
        rebuild_state.dep_satisfaction
    );
    assert_eq!(
        baseline_state.idempotency_keys,
        rebuild_state.idempotency_keys
    );
    assert_eq!(
        baseline_manifest.watermarks.last_visible_event_id,
        rebuild_manifest.watermarks.last_visible_event_id
    );

    Ok(())
}

#[tokio::test]
async fn rebuild_remains_correct_with_stale_watermark_and_without_ledger_listing() -> Result<()> {
    let backend = Arc::new(ListTrackingBackend::new());
    let storage = ScopedStorage::new(backend.clone(), TENANT, WORKSPACE)?;
    let compactor = MicroCompactor::new(storage.clone());

    let run_id = "run-dr-stale";
    let plan_id = "plan-dr-stale";

    let first_event = run_triggered("01J1DRREBUILD00000000000011", run_id, plan_id);
    let second_event = plan_created("01J1DRREBUILD00000000000012", run_id, plan_id);

    let first_path = write_event(&storage, &first_event).await?;
    let second_path = write_event(&storage, &second_event).await?;
    compactor
        .compact_events(vec![first_path.clone(), second_path.clone()])
        .await?;

    let pointer_bytes = storage.get_raw(POINTER_PATH).await?;
    let pointer: OrchestrationManifestPointer =
        serde_json::from_slice(&pointer_bytes).expect("parse pointer");
    let manifest_bytes = storage.get_raw(&pointer.manifest_path).await?;
    let mut manifest: OrchestrationManifest =
        serde_json::from_slice(&manifest_bytes).expect("parse manifest");
    manifest.watermarks.last_processed_at = Utc::now() - Duration::minutes(30);

    storage
        .put_raw(
            &pointer.manifest_path,
            Bytes::from(serde_json::to_vec(&manifest).expect("serialize manifest")),
            WritePrecondition::None,
        )
        .await?;
    storage
        .put_raw(
            LEGACY_MANIFEST_PATH,
            Bytes::from(serde_json::to_vec(&manifest).expect("serialize legacy manifest")),
            WritePrecondition::None,
        )
        .await?;

    let third_event = task_finished(
        "01J1DRREBUILD00000000000013",
        run_id,
        "01J1DRREBUILDATTEMPT000000000001",
    );
    let third_path = write_event(&storage, &third_event).await?;

    backend.reset_list_calls();

    let rebuild_manifest = LedgerRebuildManifest {
        event_paths: vec![first_path.clone(), second_path.clone(), third_path.clone()],
    };
    compactor
        .rebuild_from_ledger_manifest(rebuild_manifest, None)
        .await?;

    assert_eq!(backend.list_calls(), 0, "rebuild must not list ledger");

    let (manifest_after, state_after) = compactor.load_state().await?;
    assert_eq!(
        manifest_after.watermarks.last_visible_event_id.as_deref(),
        Some("01J1DRREBUILD00000000000013")
    );
    assert!(
        state_after
            .idempotency_keys
            .contains_key(&third_event.idempotency_key),
        "rebuild should process post-watermark events even when watermark timestamp is stale"
    );

    Ok(())
}

#[tokio::test]
async fn rebuild_rejects_invalid_event_paths_as_invalid_input() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage");
    let compactor = MicroCompactor::new(storage);

    let error = compactor
        .rebuild_from_ledger_manifest(
            LedgerRebuildManifest {
                event_paths: vec!["ledger/orchestration/2026-02-21/".to_string()],
            },
            None,
        )
        .await
        .expect_err("invalid event path must be rejected");

    assert!(matches!(
        error,
        FlowError::Core(arco_core::Error::InvalidInput(_))
    ));
}

#[tokio::test]
async fn rebuild_manifest_parse_error_is_invalid_input() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage");
    let compactor = MicroCompactor::new(storage.clone());

    let rebuild_manifest_path = "state/orchestration/rebuilds/invalid-manifest.json";
    storage
        .put_raw(
            rebuild_manifest_path,
            Bytes::from_static(br#"{"event_paths":"not-an-array"}"#),
            WritePrecondition::None,
        )
        .await
        .expect("write malformed rebuild manifest");

    let error = compactor
        .rebuild_from_ledger_manifest_path(rebuild_manifest_path, None)
        .await
        .expect_err("invalid manifest payload must be rejected");

    assert!(matches!(
        error,
        FlowError::Core(arco_core::Error::InvalidInput(_))
    ));
}
