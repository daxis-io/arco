//! End-to-end dispatch loop tests for event-driven orchestration.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use arco_core::{MemoryBackend, ScopedStorage};
use arco_flow::error::Result;
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::compactor::fold::DispatchStatus;
use arco_flow::orchestration::controllers::{DispatchAction, DispatcherController, ReadyDispatchController};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TaskDef, TriggerInfo};

#[tokio::test]
async fn dispatch_e2e_loop_creates_outbox_and_enqueued_event() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
    let ledger = LedgerWriter::new(storage.clone());
    let compactor = MicroCompactor::new(storage.clone());

    let run_id = "run_01";
    let plan_id = "plan_01";
    let task_key = "extract";

    let run_triggered = OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::RunTriggered {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            trigger: TriggerInfo::Manual {
                user_id: "tester".to_string(),
            },
            root_assets: vec![],
            run_key: None,
            labels: std::collections::HashMap::new(),
        },
    );

    let plan_created = OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::PlanCreated {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            tasks: vec![TaskDef {
                key: task_key.to_string(),
                depends_on: vec![],
                asset_key: None,
                partition_key: None,
                max_attempts: 3,
                heartbeat_timeout_sec: 300,
            }],
        },
    );

    let run_path = LedgerWriter::event_path(&run_triggered);
    ledger.append(run_triggered).await?;
    let plan_path = LedgerWriter::event_path(&plan_created);
    ledger.append(plan_created).await?;
    compactor.compact_events(vec![run_path, plan_path]).await?;

    let (manifest, state) = compactor.load_state().await?;
    let ready_dispatch = ReadyDispatchController::with_defaults();
    let actions = ready_dispatch.reconcile(&manifest, &state);
    assert_eq!(actions.len(), 1);

    let (dispatch_id, attempt_id, attempt, event_data) = match actions
        .into_iter()
        .next()
        .expect("dispatch action")
    {
        arco_flow::orchestration::controllers::ReadyDispatchAction::EmitDispatchRequested {
            run_id: action_run_id,
            task_key: action_task_key,
            attempt,
            attempt_id,
            worker_queue: action_worker_queue,
            dispatch_id,
        } => {
            assert_eq!(action_run_id, run_id);
            assert_eq!(action_task_key, task_key);
            assert_eq!(action_worker_queue, "default-queue");
            let event_data = OrchestrationEventData::DispatchRequested {
                run_id: action_run_id,
                task_key: action_task_key,
                attempt,
                attempt_id: attempt_id.clone(),
                worker_queue: action_worker_queue,
                dispatch_id: dispatch_id.clone(),
            };
            (dispatch_id, attempt_id, attempt, event_data)
        }
        _ => panic!("expected EmitDispatchRequested action"),
    };

    let dispatch_requested = OrchestrationEvent::new("tenant", "workspace", event_data);
    let dispatch_path = LedgerWriter::event_path(&dispatch_requested);
    ledger.append(dispatch_requested).await?;
    compactor.compact_events(vec![dispatch_path]).await?;

    let (manifest, state) = compactor.load_state().await?;
    let outbox_row = state.dispatch_outbox.get(&dispatch_id).expect("outbox row");
    assert_eq!(outbox_row.status, DispatchStatus::Pending);
    assert_eq!(outbox_row.attempt_id, attempt_id);

    let dispatcher = DispatcherController::with_defaults();
    let outbox_rows: Vec<_> = state.dispatch_outbox.values().cloned().collect();
    let dispatch_actions = dispatcher.reconcile(&manifest, &outbox_rows);
    assert_eq!(dispatch_actions.len(), 1);

    let cloud_task_id = match &dispatch_actions[0] {
        DispatchAction::CreateCloudTask {
            dispatch_id: action_dispatch_id,
            cloud_task_id,
            run_id: action_run_id,
            task_key: action_task_key,
            attempt: action_attempt,
            ..
        } => {
            assert_eq!(action_dispatch_id, &dispatch_id);
            assert_eq!(action_run_id, run_id);
            assert_eq!(action_task_key, task_key);
            assert_eq!(*action_attempt, attempt);
            cloud_task_id.clone()
        }
        _ => panic!("expected CreateCloudTask action"),
    };

    let dispatch_enqueued = OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::DispatchEnqueued {
            dispatch_id: dispatch_id.clone(),
            run_id: Some(run_id.to_string()),
            task_key: Some(task_key.to_string()),
            attempt: Some(attempt),
            cloud_task_id: cloud_task_id.clone(),
        },
    );
    let enqueued_path = LedgerWriter::event_path(&dispatch_enqueued);
    ledger.append(dispatch_enqueued).await?;
    compactor.compact_events(vec![enqueued_path]).await?;

    let (_manifest, state) = compactor.load_state().await?;
    let outbox_row = state.dispatch_outbox.get(&dispatch_id).expect("outbox row");
    assert_eq!(outbox_row.status, DispatchStatus::Created);
    assert_eq!(outbox_row.cloud_task_id.as_deref(), Some(cloud_task_id.as_str()));

    Ok(())
}
