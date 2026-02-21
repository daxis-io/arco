//! External worker callback end-to-end orchestration test.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::future::Future;
use std::sync::Arc;

use chrono::Utc;

use arco_core::{
    MemoryBackend, ScopedStorage, TaskTokenConfig, decode_task_token, mint_task_token,
};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::callbacks::{
    CallbackContext, CallbackResult, TaskCompletedRequest, TaskOutput, TaskStartedRequest,
    TaskState as CallbackTaskState, TaskStateLookup, TaskTokenValidator, WorkerOutcome,
    handle_task_completed, handle_task_started,
};
use arco_flow::orchestration::compactor::fold::DispatchStatus;
use arco_flow::orchestration::compactor::{MicroCompactor, TaskState as FoldTaskState};
use arco_flow::orchestration::controllers::{
    DispatchAction, DispatcherController, ReadyDispatchAction, ReadyDispatchController,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TriggerInfo,
};
use arco_flow::orchestration::ledger::OrchestrationLedgerWriter;
use arco_flow::orchestration::worker_contract::WorkerDispatchEnvelope;

#[derive(Clone)]
struct CompactingTestLedger {
    ledger: LedgerWriter,
    compactor: MicroCompactor,
}

impl OrchestrationLedgerWriter for CompactingTestLedger {
    fn write_event(
        &self,
        event: &OrchestrationEvent,
    ) -> impl Future<Output = Result<(), String>> + Send {
        let writer = self.clone();
        let event = event.clone();

        async move {
            let path = LedgerWriter::event_path(&event);
            writer
                .ledger
                .append(event)
                .await
                .map_err(|e| e.to_string())?;
            writer
                .compactor
                .compact_events(vec![path])
                .await
                .map_err(|e| e.to_string())?;
            Ok(())
        }
    }
}

#[derive(Clone)]
struct CompactorLookup {
    compactor: MicroCompactor,
}

impl TaskStateLookup for CompactorLookup {
    fn get_task_state(
        &self,
        task_id: &str,
    ) -> impl Future<Output = Result<Option<CallbackTaskState>, String>> + Send {
        let task_id = task_id.to_string();
        let compactor = self.compactor.clone();

        async move {
            let (_, state) = compactor.load_state().await.map_err(|e| e.to_string())?;
            let matches: Vec<_> = state
                .tasks
                .values()
                .filter(|row| row.task_key == task_id)
                .collect();

            if matches.is_empty() {
                return Ok(None);
            }
            if matches.len() > 1 {
                return Err(format!("task_id_ambiguous: {task_id}"));
            }

            let Some(row) = matches.first() else {
                return Ok(None);
            };
            let run = state.runs.get(&row.run_id);
            Ok(Some(CallbackTaskState {
                state: fold_task_state_label(row.state).to_string(),
                attempt: row.attempt,
                attempt_id: row.attempt_id.clone().unwrap_or_default(),
                run_id: row.run_id.clone(),
                asset_key: row.asset_key.clone(),
                partition_key: row.partition_key.clone(),
                code_version: run.and_then(|run| run.code_version.clone()),
                cancel_requested: run.is_some_and(|run| run.cancel_requested),
            }))
        }
    }
}

fn fold_task_state_label(state: FoldTaskState) -> &'static str {
    match state {
        FoldTaskState::Planned => "PLANNED",
        FoldTaskState::Blocked => "BLOCKED",
        FoldTaskState::Ready => "READY",
        FoldTaskState::Dispatched => "DISPATCHED",
        FoldTaskState::Running => "RUNNING",
        FoldTaskState::RetryWait => "RETRY_WAIT",
        FoldTaskState::Skipped => "SKIPPED",
        FoldTaskState::Cancelled => "CANCELLED",
        FoldTaskState::Failed => "FAILED",
        FoldTaskState::Succeeded => "SUCCEEDED",
    }
}

#[derive(Clone)]
struct CoreTaskTokenValidator {
    config: TaskTokenConfig,
    tenant_id: String,
    workspace_id: String,
}

impl TaskTokenValidator for CoreTaskTokenValidator {
    fn validate_task_token(
        &self,
        task_id: &str,
        _run_id: &str,
        _attempt: u32,
        token: &str,
    ) -> impl Future<Output = Result<(), String>> + Send {
        let config = self.config.clone();
        let tenant_id = self.tenant_id.clone();
        let workspace_id = self.workspace_id.clone();
        let task_id = task_id.to_string();
        let token = token.to_string();

        async move {
            let claims = decode_task_token(&config, &token).map_err(|e| e.to_string())?;
            if claims.task_id != task_id {
                return Err("task_id_mismatch".to_string());
            }
            if claims.tenant_id != tenant_id {
                return Err("tenant_mismatch".to_string());
            }
            if claims.workspace_id != workspace_id {
                return Err("workspace_mismatch".to_string());
            }
            Ok(())
        }
    }
}

#[tokio::test]
async fn run_dispatch_callback_path_advances_task_state() {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
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
            code_version: None,
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
    let plan_path = LedgerWriter::event_path(&plan_created);
    ledger.append(run_triggered).await.expect("append run");
    ledger.append(plan_created).await.expect("append plan");
    compactor
        .compact_events(vec![run_path, plan_path])
        .await
        .expect("compact initial events");

    let (manifest, state) = compactor.load_state().await.expect("load state");
    let ready_actions = ReadyDispatchController::with_defaults().reconcile(&manifest, &state);
    assert_eq!(ready_actions.len(), 1);

    let (dispatch_id, attempt_id, attempt, dispatch_event_data) =
        match ready_actions.into_iter().next().expect("ready action") {
            ReadyDispatchAction::EmitDispatchRequested {
                run_id: action_run_id,
                task_key: action_task_key,
                attempt,
                attempt_id,
                worker_queue,
                dispatch_id,
            } => {
                assert_eq!(action_run_id, run_id);
                assert_eq!(action_task_key, task_key);
                assert_eq!(worker_queue, "default-queue");

                let data = OrchestrationEventData::DispatchRequested {
                    run_id: action_run_id,
                    task_key: action_task_key,
                    attempt,
                    attempt_id: attempt_id.clone(),
                    worker_queue,
                    dispatch_id: dispatch_id.clone(),
                };
                (dispatch_id, attempt_id, attempt, data)
            }
            _ => panic!("expected dispatch request"),
        };

    let dispatch_requested = OrchestrationEvent::new("tenant", "workspace", dispatch_event_data);
    let dispatch_requested_path = LedgerWriter::event_path(&dispatch_requested);
    ledger
        .append(dispatch_requested)
        .await
        .expect("append dispatch requested");
    compactor
        .compact_events(vec![dispatch_requested_path])
        .await
        .expect("compact dispatch requested");

    let (manifest, state) = compactor.load_state().await.expect("load state");
    let outbox_rows: Vec<_> = state.dispatch_outbox.values().cloned().collect();
    let actions = DispatcherController::with_defaults().reconcile(&manifest, &outbox_rows);
    assert_eq!(actions.len(), 1);

    let cloud_task_id = match &actions[0] {
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
        _ => panic!("expected cloud task dispatch action"),
    };

    let token_config = TaskTokenConfig {
        hs256_secret: "test-task-secret".to_string(),
        issuer: Some("https://issuer.task".to_string()),
        audience: Some("arco-worker-callback".to_string()),
        ttl_seconds: 900,
    };
    let minted = mint_task_token(&token_config, task_key, "tenant", "workspace", Utc::now())
        .expect("mint token");

    let envelope = WorkerDispatchEnvelope {
        tenant_id: "tenant".to_string(),
        workspace_id: "workspace".to_string(),
        run_id: run_id.to_string(),
        task_key: task_key.to_string(),
        attempt,
        attempt_id: attempt_id.clone(),
        dispatch_id: dispatch_id.clone(),
        worker_queue: "default-queue".to_string(),
        callback_base_url: "https://api.arco.dev".to_string(),
        task_token: minted.token.clone(),
        token_expires_at: minted.expires_at,
        traceparent: None,
        payload: serde_json::json!({}),
    };
    assert_eq!(envelope.dispatch_id, dispatch_id);

    let dispatch_enqueued = OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::DispatchEnqueued {
            dispatch_id: dispatch_id.clone(),
            run_id: Some(run_id.to_string()),
            task_key: Some(task_key.to_string()),
            attempt: Some(attempt),
            cloud_task_id,
        },
    );
    let dispatch_enqueued_path = LedgerWriter::event_path(&dispatch_enqueued);
    ledger
        .append(dispatch_enqueued)
        .await
        .expect("append dispatch enqueued");
    compactor
        .compact_events(vec![dispatch_enqueued_path])
        .await
        .expect("compact dispatch enqueued");

    let callback_ctx = CallbackContext::new(
        Arc::new(CompactingTestLedger {
            ledger: ledger.clone(),
            compactor: compactor.clone(),
        }),
        Arc::new(CoreTaskTokenValidator {
            config: token_config,
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
        }),
        "tenant",
        "workspace",
    );
    let lookup = CompactorLookup {
        compactor: compactor.clone(),
    };

    let started = handle_task_started(
        &callback_ctx,
        task_key,
        &envelope.task_token,
        TaskStartedRequest {
            attempt,
            attempt_id: attempt_id.clone(),
            worker_id: "worker-1".to_string(),
            traceparent: None,
            started_at: Some(Utc::now()),
        },
        &lookup,
    )
    .await;
    assert!(matches!(started, CallbackResult::Ok(_)));

    let completed = handle_task_completed(
        &callback_ctx,
        task_key,
        &envelope.task_token,
        TaskCompletedRequest {
            attempt,
            attempt_id: attempt_id.clone(),
            worker_id: "worker-1".to_string(),
            traceparent: None,
            outcome: WorkerOutcome::Succeeded,
            completed_at: Some(Utc::now()),
            output: Some(TaskOutput {
                materialization_id: Some("mat-1".to_string()),
                row_count: Some(10),
                byte_size: Some(100),
                output_path: Some("s3://bucket/path".to_string()),
            }),
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
        },
        &lookup,
    )
    .await;
    assert!(matches!(completed, CallbackResult::Ok(_)));

    let (_manifest, state) = compactor.load_state().await.expect("load final state");
    let task = state
        .tasks
        .values()
        .find(|row| row.task_key == task_key)
        .expect("task row");
    assert_eq!(task.state, FoldTaskState::Succeeded);

    let outbox = state
        .dispatch_outbox
        .get(&dispatch_id)
        .expect("outbox row present");
    assert_eq!(outbox.status, DispatchStatus::Created);
}
