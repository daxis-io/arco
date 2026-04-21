//! Tests for visible orchestration state used by system tables.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use chrono::Utc;
use ulid::Ulid;

use arco_core::storage::WritePrecondition;
use arco_core::{MemoryBackend, ScopedStorage};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::{
    MicroCompactor, OrchestrationManifest, OrchestrationManifestPointer,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TaskOutcome, TriggerInfo,
};
use arco_flow::orchestration_manifest_pointer_path;

#[tokio::test]
async fn load_state_reads_visible_l0_state_without_base_snapshot() -> Result<()> {
    let compactor = seeded_compactor().await?;
    let (_, state) = compactor.load_state().await?;
    assert!(state.runs.contains_key("run_01"));
    assert!(
        state
            .tasks
            .contains_key(&("run_01".to_string(), "extract".to_string()))
    );
    Ok(())
}

async fn seeded_compactor() -> Result<MicroCompactor> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
    seed_visible_l0_state(&storage).await?;
    Ok(MicroCompactor::new(storage))
}

async fn seed_visible_l0_state(storage: &ScopedStorage) -> Result<()> {
    let manifest_id = "00000000000000000000";
    let manifest_path = format!("state/orchestration/manifests/{manifest_id}.json");

    let manifest = OrchestrationManifest::new(Ulid::new().to_string());

    let manifest_bytes = serde_json::to_vec(&manifest)
        .map(Bytes::from)
        .map_err(|err| Error::serialization(format!("serialize seeded manifest: {err}")))?;
    storage
        .put_raw(
            &manifest_path,
            manifest_bytes,
            WritePrecondition::DoesNotExist,
        )
        .await?;

    let pointer = OrchestrationManifestPointer {
        manifest_id: manifest.manifest_id.clone(),
        manifest_path,
        epoch: 0,
        parent_pointer_hash: None,
        updated_at: Utc::now(),
    };
    let pointer_bytes = serde_json::to_vec(&pointer)
        .map(Bytes::from)
        .map_err(|err| Error::serialization(format!("serialize seeded pointer: {err}")))?;
    storage
        .put_raw(
            orchestration_manifest_pointer_path(),
            pointer_bytes,
            WritePrecondition::DoesNotExist,
        )
        .await?;

    let run_id = "run_01";
    let plan_id = "plan_01";
    let task_key = "extract";
    let attempt_id = "attempt_01";

    let events = vec![
        OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::RunTriggered {
                run_id: run_id.to_string(),
                plan_id: plan_id.to_string(),
                trigger: TriggerInfo::Manual {
                    user_id: "tester@example.com".to_string(),
                },
                root_assets: vec!["analytics.daily".to_string()],
                run_key: None,
                labels: HashMap::new(),
                code_version: None,
            },
        ),
        OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::PlanCreated {
                run_id: run_id.to_string(),
                plan_id: plan_id.to_string(),
                tasks: vec![TaskDef {
                    key: task_key.to_string(),
                    depends_on: vec![],
                    asset_key: Some("analytics.daily".to_string()),
                    partition_key: Some("2025-01-15".to_string()),
                    max_attempts: 1,
                    heartbeat_timeout_sec: 300,
                }],
            },
        ),
        OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::TaskStarted {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt: 1,
                attempt_id: attempt_id.to_string(),
                worker_id: "worker-1".to_string(),
            },
        ),
        OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::TaskFinished {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt: 1,
                attempt_id: attempt_id.to_string(),
                worker_id: "worker-1".to_string(),
                outcome: TaskOutcome::Succeeded,
                materialization_id: Some("mat_01".to_string()),
                error_message: None,
                output: None,
                error: None,
                metrics: None,
                cancelled_during_phase: None,
                partial_progress_json: None,
                asset_key: Some("analytics.daily".to_string()),
                partition_key: Some("2025-01-15".to_string()),
                code_version: None,
            },
        ),
    ];

    let ledger = LedgerWriter::new(storage.clone());
    let paths: Vec<_> = events.iter().map(LedgerWriter::event_path).collect();
    ledger.append_all(events).await?;

    MicroCompactor::new(storage.clone())
        .compact_events(paths)
        .await?;
    Ok(())
}
