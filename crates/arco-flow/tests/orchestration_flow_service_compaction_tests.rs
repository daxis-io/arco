//! Flow service orchestration compaction integration tests.

#![cfg(any(feature = "gcp", feature = "test-utils"))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};

use arco_core::{MemoryBackend, ScopedStorage};
use arco_flow::error::Result;
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::controllers::{
    DispatchAction, DispatcherController, ReadyDispatchController,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TriggerInfo,
};
use arco_flow::orchestration::flow_service::append_events_and_compact;
use arco_flow::orchestration_compaction::{
    OrchestrationCompactRequest, OrchestrationCompactionResponse,
};

#[derive(Clone)]
struct CompactorState {
    compactor: MicroCompactor,
    captured_request: Arc<Mutex<Option<OrchestrationCompactRequest>>>,
}

async fn compact_handler(
    State(state): State<CompactorState>,
    Json(req): Json<OrchestrationCompactRequest>,
) -> std::result::Result<Json<OrchestrationCompactionResponse>, axum::http::StatusCode> {
    *state.captured_request.lock().expect("capture lock") = Some(req.clone());
    let result = state
        .compactor
        .compact_events_fenced(req.event_paths, req.fencing_token, &req.lock_path)
        .await
        .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(OrchestrationCompactionResponse {
        events_processed: result.events_processed,
        delta_id: result.delta_id,
        manifest_revision: result.manifest_revision,
        visibility_status: result.visibility_status.into(),
        repair_pending: false,
    }))
}

async fn start_compactor_server(
    compactor: MicroCompactor,
) -> (
    String,
    Arc<Mutex<Option<OrchestrationCompactRequest>>>,
    tokio::task::JoinHandle<()>,
) {
    let captured_request = Arc::new(Mutex::new(None));
    let app = Router::new()
        .route("/compact", post(compact_handler))
        .with_state(CompactorState {
            compactor,
            captured_request: captured_request.clone(),
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind compactor listener");
    let addr: SocketAddr = listener.local_addr().expect("listener addr");
    let base_url = format!("http://{addr}");

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve compactor");
    });

    (base_url, captured_request, handle)
}

#[tokio::test]
async fn dispatcher_compacts_emitted_events_to_prevent_ledger_spam() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let ledger = LedgerWriter::new(storage.clone());
    let compactor = MicroCompactor::new(storage.clone());

    let (compactor_url, captured_request, _handle) =
        start_compactor_server(compactor.clone()).await;

    // Seed a pending dispatch outbox row.
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
                requires_visible_output: false,
            }],
        },
    );

    let run_path = LedgerWriter::event_path(&run_triggered);
    let plan_path = LedgerWriter::event_path(&plan_created);
    ledger.append(run_triggered).await?;
    ledger.append(plan_created).await?;
    compactor.compact_events(vec![run_path, plan_path]).await?;

    let (manifest, state) = compactor.load_state().await?;
    let ready_dispatch = ReadyDispatchController::with_defaults();
    let ready_actions = ready_dispatch.reconcile(&manifest, &state);
    assert_eq!(ready_actions.len(), 1);

    let dispatch_requested = match ready_actions.into_iter().next().expect("ready action") {
        arco_flow::orchestration::controllers::ReadyDispatchAction::EmitDispatchRequested {
            run_id,
            task_key,
            attempt,
            attempt_id,
            worker_queue,
            dispatch_id,
        } => OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::DispatchRequested {
                run_id,
                task_key,
                attempt,
                attempt_id,
                worker_queue,
                dispatch_id,
            },
        ),
        _ => panic!("expected EmitDispatchRequested action"),
    };

    let requested_path = LedgerWriter::event_path(&dispatch_requested);
    ledger.append(dispatch_requested).await?;
    compactor.compact_events(vec![requested_path]).await?;

    // Simulate the dispatcher loop twice. Without post-append compaction, the second
    // run would still see the outbox row as PENDING and emit a duplicate DispatchEnqueued.
    let mut emitted_counts = Vec::new();
    for _ in 0..2 {
        let (manifest, state) = compactor.load_state().await?;
        let outbox_rows: Vec<_> = state.dispatch_outbox.values().cloned().collect();
        let dispatcher = DispatcherController::with_defaults();
        let actions = dispatcher.reconcile(&manifest, &outbox_rows);

        let mut events = Vec::new();
        for action in actions {
            let DispatchAction::CreateCloudTask {
                dispatch_id,
                cloud_task_id,
                run_id,
                task_key,
                attempt,
                ..
            } = action
            else {
                continue;
            };

            events.push(OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::DispatchEnqueued {
                    dispatch_id,
                    run_id: Some(run_id),
                    task_key: Some(task_key),
                    attempt: Some(attempt),
                    cloud_task_id,
                },
            ));
        }

        let emitted = events.len();
        append_events_and_compact(&ledger, Some(compactor_url.as_str()), events).await?;
        emitted_counts.push(emitted);
    }

    assert_eq!(emitted_counts, vec![1, 0]);

    let request = captured_request
        .lock()
        .expect("capture lock")
        .clone()
        .expect("captured fenced request");
    assert!(request.fencing_token > 0);
    assert_eq!(
        request.lock_path,
        "locks/orchestration.compaction.lock.json"
    );
    Ok(())
}
