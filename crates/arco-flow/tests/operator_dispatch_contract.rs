//! Operator-owned queues receive the same worker envelope as the packaged dispatcher.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Mutex;

use arco_core::{ControlPlaneScope, MemoryBackend, ScopedStorage, StorageBackend, TaskTokenConfig};
use arco_flow::dispatch::{
    EnqueueOptions, EnqueueResult, HttpTaskEnqueuer, enqueue_worker_dispatch,
};
use arco_flow::error::Result;
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::dispatcher_service::{
    DispatcherServiceConfig, DispatcherServiceState, router,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TimerType, TriggerInfo,
};
use arco_flow::orchestration::flow_service::append_events_and_compact;
use arco_flow::orchestration::sweeper_service::{
    SweeperServiceConfig, SweeperServiceState, router as sweeper_router,
};
use arco_worker_contract::WorkerDispatchEnvelope;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::Utc;
use std::sync::Arc;
use tower::ServiceExt;

#[derive(Debug)]
struct SentTask {
    id: String,
    target_url: String,
    body: serde_json::Value,
    routing_key: Option<String>,
    audience: Option<String>,
    headers: HashMap<String, String>,
}

#[derive(Default)]
struct OperatorQueue {
    sent: Mutex<Option<SentTask>>,
}

#[async_trait::async_trait]
impl HttpTaskEnqueuer for OperatorQueue {
    async fn enqueue_http(
        &self,
        task_id: &str,
        target_url: &str,
        body: &[u8],
        options: EnqueueOptions,
        audience: Option<&str>,
        extra_headers: Option<HashMap<String, String>>,
    ) -> Result<EnqueueResult> {
        let sent = SentTask {
            id: task_id.to_string(),
            target_url: target_url.to_string(),
            body: serde_json::from_slice(body).expect("canonical worker JSON"),
            routing_key: options.routing_key,
            audience: audience.map(str::to_string),
            headers: extra_headers.expect("worker headers"),
        };
        *self.sent.lock().expect("operator queue lock") = Some(sent);
        Ok(EnqueueResult::Enqueued {
            message_id: task_id.to_string(),
        })
    }
}

#[tokio::test]
async fn operator_queue_receives_the_canonical_worker_envelope() {
    let queue = OperatorQueue::default();
    let envelope = WorkerDispatchEnvelope {
        tenant_id: "tenant-a".into(),
        workspace_id: "workspace-b".into(),
        task_id: "task-1".into(),
        task_key: "sales.daily".into(),
        run_id: "run-1".into(),
        attempt: 2,
        attempt_id: "attempt-2".into(),
        dispatch_id: "dispatch-2".into(),
        execution_location_id: None,
        partition_key: Some("date=2026-09-29".into()),
        heartbeat_timeout_sec: Some(300),
        worker_queue: "priority".into(),
        callback_base_url: "https://api.example/callback".into(),
        task_token: "scoped-token".into(),
        token_expires_at: Utc::now(),
        traceparent: None,
        payload: serde_json::json!({"artifact": "orders"}),
    };
    let headers = HashMap::from([("X-Arco-Dispatch-Secret".into(), "secret".into())]);

    let result = enqueue_worker_dispatch(
        &queue,
        "queue-task-2",
        "https://worker.example/dispatch",
        "https://worker.example",
        &headers,
        &envelope,
    )
    .await
    .expect("operator queue accepts task");

    assert!(matches!(result, EnqueueResult::Enqueued { .. }));
    let sent = queue
        .sent
        .into_inner()
        .expect("operator queue lock")
        .expect("task was handed off");
    assert_eq!(sent.id, "queue-task-2");
    assert_eq!(sent.target_url, "https://worker.example/dispatch");
    assert_eq!(sent.routing_key.as_deref(), Some("priority"));
    assert_eq!(sent.audience.as_deref(), Some("https://worker.example"));
    assert_eq!(sent.headers, headers);
    assert_eq!(sent.body["dispatchId"], "dispatch-2");
    assert_eq!(sent.body["partitionKey"], "date=2026-09-29");
    assert_eq!(sent.body["heartbeatTimeoutSec"], 300);
    assert_eq!(sent.body["taskToken"], "scoped-token");
}

#[tokio::test]
async fn operator_can_host_the_real_dispatcher_router() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
    let ledger = LedgerWriter::new(storage.clone());
    append_events_and_compact(
        &ledger,
        None,
        vec![
            OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::RunTriggered {
                    run_id: "run".into(),
                    plan_id: "plan".into(),
                    trigger: TriggerInfo::Manual {
                        user_id: "operator".into(),
                    },
                    root_assets: vec!["task".into()],
                    run_key: None,
                    labels: HashMap::new(),
                    code_version: None,
                },
            ),
            OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::PlanCreated {
                    run_id: "run".into(),
                    plan_id: "plan".into(),
                    tasks: vec![TaskDef {
                        key: "task".into(),
                        depends_on: vec![],
                        asset_key: None,
                        partition_key: None,
                        max_attempts: 1,
                        heartbeat_timeout_sec: 300,
                        requires_visible_output: false,
                    }],
                },
            ),
            OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::DispatchRequested {
                    run_id: "run".into(),
                    task_key: "task".into(),
                    attempt: 1,
                    attempt_id: "attempt".into(),
                    worker_queue: "default-queue".into(),
                    dispatch_id: "dispatch".into(),
                },
            ),
        ],
    )
    .await
    .expect("seed outbox");

    let queue = Arc::new(OperatorQueue::default());
    let state = DispatcherServiceState::new(storage, queue.clone(), dispatch_config())
        .expect("workspace dispatcher");
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/run")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let sent = queue.sent.lock().expect("queue lock");
    assert_eq!(sent.as_ref().expect("task").body["dispatchId"], "dispatch");
    drop(sent);
}

#[tokio::test]
async fn http_mode_refuses_timer_without_an_enqueue_acknowledgement() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
    append_events_and_compact(
        &LedgerWriter::new(storage.clone()),
        None,
        vec![OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::TimerRequested {
                timer_id: "timer-1".into(),
                timer_type: TimerType::Retry,
                run_id: Some("run".into()),
                task_key: Some("task".into()),
                attempt: Some(1),
                fire_at: Utc::now() + chrono::Duration::minutes(5),
            },
        )],
    )
    .await
    .expect("seed timer");
    let queue = Arc::new(OperatorQueue::default());
    let state = DispatcherServiceState::new(storage.clone(), queue.clone(), dispatch_config())
        .expect("dispatcher");
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/run")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let summary: serde_json::Value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body"),
    )
    .expect("summary");
    assert_eq!(summary["timer_failed"], 1);
    assert_eq!(summary["timer_enqueued"], 0);
    assert!(queue.sent.lock().expect("queue").is_none());
    let (_, fold) = MicroCompactor::new(storage)
        .load_state()
        .await
        .expect("fold");
    assert!(fold.timers["timer-1"].cloud_task_id.is_none());
}

fn dispatch_config() -> DispatcherServiceConfig {
    DispatcherServiceConfig {
        orch_compactor_url: None,
        worker_dispatch_headers: HashMap::new(),
        dispatch_target_url: "https://worker.example/dispatch".into(),
        dispatch_target_audience: "https://worker.example".into(),
        callback_base_url: "https://api.example".into(),
        task_token_config: TaskTokenConfig {
            hs256_secret: "test-task-token-secret-32-bytes!!".into(),
            issuer: Some("issuer".into()),
            audience: Some("audience".into()),
            ttl_seconds: 3600,
        },
        task_timeout_secs: 1800,
        timer_target_url: None,
        timer_target_audience: None,
        timer_queue: None,
    }
}

#[test]
fn operator_dispatcher_rejects_metastore_root() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let scope = ControlPlaneScope::workspace_alias("tenant", "workspace").expect("scope");
    let storage = ScopedStorage::new_metastore_scoped(backend, &scope).expect("metastore storage");
    let error = DispatcherServiceState::new(
        storage,
        Arc::new(OperatorQueue::default()),
        dispatch_config(),
    )
    .err()
    .expect("metastore root must be rejected");
    assert!(matches!(
        error,
        arco_flow::error::Error::Configuration { .. }
    ));
}

#[test]
fn operator_sweeper_accepts_a_queue_and_rejects_metastore_root() {
    let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let workspace = ScopedStorage::new(backend.clone(), "tenant", "workspace").expect("storage");
    let state = SweeperServiceState::new(
        workspace,
        Arc::new(OperatorQueue::default()),
        SweeperServiceConfig {
            orch_compactor_url: None,
            worker_dispatch_headers: HashMap::new(),
            dispatch_target_url: "https://worker.example/dispatch".into(),
            dispatch_target_audience: "https://worker.example".into(),
            callback_base_url: "https://api.example".into(),
            task_token_config: dispatch_config().task_token_config,
            task_timeout_secs: 1800,
        },
    )
    .expect("workspace sweeper");
    let _router = sweeper_router(state);

    let scope = ControlPlaneScope::workspace_alias("tenant", "workspace").expect("scope");
    let metastore = ScopedStorage::new_metastore_scoped(backend, &scope).expect("metastore");
    let error = SweeperServiceState::new(
        metastore,
        Arc::new(OperatorQueue::default()),
        SweeperServiceConfig {
            orch_compactor_url: None,
            worker_dispatch_headers: HashMap::new(),
            dispatch_target_url: "https://worker.example/dispatch".into(),
            dispatch_target_audience: "https://worker.example".into(),
            callback_base_url: "https://api.example".into(),
            task_token_config: dispatch_config().task_token_config,
            task_timeout_secs: 1800,
        },
    )
    .err()
    .expect("metastore root rejected");
    assert!(matches!(
        error,
        arco_flow::error::Error::Configuration { .. }
    ));
}
