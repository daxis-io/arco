//! Single-host operator dispatcher example. See the operator dispatch runbook.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arco_core::observability::{LogFormat, init_logging};
use arco_core::{
    DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS, DEFAULT_TASK_TOKEN_TTL_SECONDS, ScopedStorage,
    TaskTokenConfig,
};
use arco_flow::dispatch::worker_auth::worker_dispatch_headers;
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::dispatcher_service::{
    DispatcherServiceConfig, DispatcherServiceState, router,
};
use arco_flow::orchestration::sweeper_service::{
    SweeperServiceConfig, SweeperServiceState, router as sweeper_router,
};
use arco_storage::from_bucket;

#[path = "operator_owned_dispatcher/file_queue.rs"]
mod file_queue;
#[cfg(test)]
#[path = "operator_owned_dispatcher/persistent_flow_backend.rs"]
mod persistent_flow_backend;

use file_queue::FileQueue;

fn required(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::configuration(format!("missing {key}")))
}

fn optional(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn seconds(key: &str, default: u64) -> Result<u64> {
    optional(key).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| Error::configuration(format!("invalid {key}")))
    })
}

fn parse_bind(value: &str) -> Result<SocketAddr> {
    let bind: SocketAddr = value
        .parse()
        .map_err(|e| Error::configuration(format!("invalid ARCO_OPERATOR_BIND: {e}")))?;
    if !bind.ip().is_loopback() {
        return Err(Error::configuration(
            "ARCO_OPERATOR_BIND must use loopback; put an authenticated proxy in front",
        ));
    }
    Ok(bind)
}

fn operator_router(
    dispatcher: DispatcherServiceState,
    sweeper: SweeperServiceState,
) -> axum::Router {
    router(dispatcher).nest("/sweeper", sweeper_router(sweeper))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    init_logging(LogFormat::Pretty);
    let tenant = required("ARCO_TENANT_ID")?;
    let workspace = required("ARCO_WORKSPACE_ID")?;
    let bucket = required("ARCO_STORAGE_BUCKET")?;
    let target = required("ARCO_FLOW_DISPATCH_TARGET_URL")?;
    let queue_root = required("ARCO_OPERATOR_QUEUE_DIR")?;
    let callback_base_url = required("ARCO_FLOW_CALLBACK_BASE_URL")?;
    let timeout = seconds(
        "ARCO_FLOW_TASK_TIMEOUT_SECS",
        DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS,
    )?;
    let task_token_config = TaskTokenConfig {
        hs256_secret: required("ARCO_FLOW_TASK_TOKEN_SECRET")?,
        issuer: Some(required("ARCO_FLOW_TASK_TOKEN_ISSUER")?),
        audience: Some(required("ARCO_FLOW_TASK_TOKEN_AUDIENCE")?),
        ttl_seconds: seconds(
            "ARCO_FLOW_TASK_TOKEN_TTL_SECS",
            DEFAULT_TASK_TOKEN_TTL_SECONDS,
        )?,
    };
    task_token_config
        .validate_for_dispatch(timeout, true)
        .map_err(|e| Error::configuration(e.to_string()))?;
    let bind = parse_bind(
        &optional("ARCO_OPERATOR_BIND").unwrap_or_else(|| "127.0.0.1:8080".to_string()),
    )?;
    let queue = Arc::new(FileQueue::open(Path::new(&queue_root))?);
    let backend = from_bucket(&bucket)?;
    let storage = ScopedStorage::new(backend, tenant.clone(), workspace.clone())?;
    let dispatch_config = DispatcherServiceConfig {
        orch_compactor_url: optional("ARCO_FLOW_COMPACTOR_URL"),
        worker_dispatch_headers: worker_dispatch_headers(&required(
            "ARCO_FLOW_WORKER_DISPATCH_SECRET",
        )?)?,
        dispatch_target_url: target.clone(),
        dispatch_target_audience: optional("ARCO_FLOW_DISPATCH_TARGET_AUDIENCE").unwrap_or(target),
        callback_base_url,
        task_token_config,
        task_timeout_secs: timeout,
        timer_target_url: optional("ARCO_FLOW_TIMER_TARGET_URL"),
        timer_target_audience: optional("ARCO_FLOW_TIMER_TARGET_AUDIENCE"),
        timer_queue: None,
    };
    let sweeper_config = SweeperServiceConfig {
        orch_compactor_url: dispatch_config.orch_compactor_url.clone(),
        worker_dispatch_headers: dispatch_config.worker_dispatch_headers.clone(),
        dispatch_target_url: dispatch_config.dispatch_target_url.clone(),
        dispatch_target_audience: dispatch_config.dispatch_target_audience.clone(),
        callback_base_url: dispatch_config.callback_base_url.clone(),
        task_token_config: dispatch_config.task_token_config.clone(),
        task_timeout_secs: timeout,
    };
    let state = DispatcherServiceState::new(storage.clone(), queue.clone(), dispatch_config)?;
    let sweeper = SweeperServiceState::new(storage, queue.clone(), sweeper_config)?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(timeout))
        .build()
        .map_err(|e| Error::configuration(format!("worker client: {e}")))?;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| Error::configuration(format!("bind dispatcher: {e}")))?;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            if let Err(error) = queue.drain_once(&client).await {
                tracing::warn!(%error, "operator queue delivery will retry");
            }
        }
    });

    axum::serve(listener, operator_router(state, sweeper))
        .await
        .map_err(|e| Error::dispatch(format!("serve dispatcher: {e}")))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use arco_core::{FlowPaths, decode_task_token, mint_task_token_for_attempt};
    use arco_core::{MemoryBackend, StorageBackend};
    use arco_flow::dispatch::OperatorHttpEnqueuer;
    use arco_flow::dispatch::{EnqueueOptions, EnqueueResult, HttpTaskEnqueuer};
    use arco_flow::orchestration::LedgerWriter;
    use arco_flow::orchestration::callbacks::{
        CallbackContext, CallbackResult, TaskCompletedRequest, TaskState as CallbackTaskState,
        TaskStateLookup, TaskTokenValidator, WorkerOutcome, handle_task_completed,
    };
    use arco_flow::orchestration::compactor::MicroCompactor;
    use arco_flow::orchestration::compactor::fold::TaskState;
    use arco_flow::orchestration::events::{
        OrchestrationEvent, OrchestrationEventData, TaskDef, TriggerInfo,
    };
    use arco_flow::orchestration::flow_service::append_events_and_compact;
    use arco_flow::orchestration::sweeper_service::{
        SweeperServiceConfig, SweeperServiceState, router as sweeper_router,
    };
    use arco_worker_contract::{WorkerDispatchEnvelope, callback_task_id};
    use axum::body::Body;
    use axum::http::{HeaderMap, Request};
    use axum::routing::post;
    use axum::{Router, http::StatusCode};
    use tower::ServiceExt;

    use super::file_queue::FileQueue;
    use super::persistent_flow_backend::PersistentFlowBackend;
    use super::*;

    struct LostFirstResponse {
        inner: Arc<FileQueue>,
        first: AtomicBool,
    }

    struct TokenValidator(TaskTokenConfig);

    impl TaskTokenValidator for TokenValidator {
        async fn validate_task_token(
            &self,
            task_id: &str,
            run_id: &str,
            attempt: u32,
            attempt_id: &str,
            token: &str,
        ) -> std::result::Result<(), String> {
            let claims = decode_task_token(&self.0, token).map_err(|error| error.to_string())?;
            if claims.task_id != task_id
                || claims.run_id.as_deref() != Some(run_id)
                || claims.attempt != Some(attempt)
                || claims.attempt_id.as_deref() != Some(attempt_id)
                || claims.tenant_id != "tenant"
                || claims.workspace_id != "workspace"
            {
                return Err("task token scope mismatch".to_string());
            }
            Ok(())
        }
    }

    struct CurrentTask(CallbackTaskState);

    impl TaskStateLookup for CurrentTask {
        fn get_task_state(
            &self,
            _task_id: &str,
        ) -> impl Future<Output = std::result::Result<Option<CallbackTaskState>, String>> + Send
        {
            let task = self.0.clone();
            async move { Ok(Some(task)) }
        }
    }

    #[async_trait::async_trait]
    impl HttpTaskEnqueuer for LostFirstResponse {
        async fn enqueue_http(
            &self,
            task_id: &str,
            target_url: &str,
            body: &[u8],
            options: EnqueueOptions,
            audience: Option<&str>,
            headers: Option<HashMap<String, String>>,
        ) -> Result<EnqueueResult> {
            let result = self
                .inner
                .enqueue_http(task_id, target_url, body, options, audience, headers)
                .await?;
            if self.first.swap(false, Ordering::SeqCst) {
                return Err(Error::dispatch("simulated lost acceptance response"));
            }
            Ok(result)
        }
    }

    #[test]
    fn dispatcher_control_route_binds_only_to_loopback() {
        assert!(parse_bind("127.0.0.1:8080").is_ok());
        assert!(parse_bind("0.0.0.0:8080").is_err());
    }

    #[tokio::test]
    async fn operator_example_hosts_the_sweeper_run_route() {
        let root =
            std::env::temp_dir().join(format!("arco-flow-sweeper-route-{}", ulid::Ulid::new()));
        let queue = Arc::new(FileQueue::open(&root).expect("queue"));
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        let token_config = TaskTokenConfig {
            hs256_secret: "test-task-token-secret-32-bytes!!".into(),
            issuer: Some("issuer".into()),
            audience: Some("audience".into()),
            ttl_seconds: 3_600,
        };
        let dispatcher = DispatcherServiceState::new(
            storage.clone(),
            queue.clone(),
            DispatcherServiceConfig {
                orch_compactor_url: None,
                worker_dispatch_headers: HashMap::new(),
                dispatch_target_url: "https://worker.example/dispatch".into(),
                dispatch_target_audience: "https://worker.example".into(),
                callback_base_url: "https://api.example".into(),
                task_token_config: token_config.clone(),
                task_timeout_secs: 1_800,
                timer_target_url: None,
                timer_target_audience: None,
                timer_queue: None,
            },
        )
        .expect("dispatcher");
        let sweeper = SweeperServiceState::new(
            storage,
            queue,
            SweeperServiceConfig {
                orch_compactor_url: None,
                worker_dispatch_headers: HashMap::new(),
                dispatch_target_url: "https://worker.example/dispatch".into(),
                dispatch_target_audience: "https://worker.example".into(),
                callback_base_url: "https://api.example".into(),
                task_token_config: token_config,
                task_timeout_secs: 1_800,
            },
        )
        .expect("sweeper");
        let response = operator_router(dispatcher, sweeper)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sweeper/run")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        std::fs::remove_dir_all(root).expect("remove queue");
    }

    #[tokio::test]
    async fn flow_state_reopens_from_disk_with_the_same_authority() {
        let root = std::env::temp_dir().join(format!("arco-flow-reopen-{}", ulid::Ulid::new()));
        std::fs::create_dir(&root).expect("state dir");
        let state_file = root.join("flow.json");
        let backend: Arc<dyn StorageBackend> =
            Arc::new(PersistentFlowBackend::open(&state_file).expect("disk backend"));
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("storage");
        append_events_and_compact(
            &LedgerWriter::new(storage.clone()),
            None,
            vec![OrchestrationEvent::new(
                "tenant",
                "workspace",
                OrchestrationEventData::RunTriggered {
                    run_id: "reopened".into(),
                    plan_id: "plan".into(),
                    trigger: TriggerInfo::Manual {
                        user_id: "operator".into(),
                    },
                    root_assets: vec![],
                    run_key: None,
                    labels: HashMap::new(),
                    code_version: None,
                },
            )],
        )
        .await
        .expect("persist Flow event");
        drop(storage);
        let backend: Arc<dyn StorageBackend> =
            Arc::new(PersistentFlowBackend::open(&state_file).expect("reopen disk backend"));
        let reopened = ScopedStorage::new(backend, "tenant", "workspace").expect("reopen scope");
        let (_, fold) = MicroCompactor::new(reopened)
            .load_state()
            .await
            .expect("reopen Flow state");
        assert!(fold.runs.contains_key("reopened"));
        std::fs::remove_dir_all(root).expect("remove state dir");
    }

    #[tokio::test]
    #[ignore = "launched by the recovery test in a separate process"]
    async fn recovery_reopen_child() {
        let root =
            std::path::PathBuf::from(std::env::var("ARCO_FLOW_RECOVERY_ROOT").expect("root"));
        let backend: Arc<dyn StorageBackend> = Arc::new(
            PersistentFlowBackend::open(&root.join("flow.json")).expect("reopen Flow state"),
        );
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scope");
        let (_, fold) = MicroCompactor::new(storage)
            .load_state()
            .await
            .expect("fold");
        assert_eq!(
            fold.tasks
                .get(&("run".into(), "task".into()))
                .expect("task")
                .state,
            TaskState::Dispatched
        );
        assert_eq!(
            std::fs::read_dir(root.join("queue/pending"))
                .expect("pending")
                .count(),
            1
        );
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the recovery proof keeps the full route sequence visible"
    )]
    async fn failed_delivery_and_expired_token_recover_through_sweeper_after_restart() {
        use std::sync::atomic::AtomicUsize;

        let root = std::env::temp_dir().join(format!("arco-flow-recovery-{}", ulid::Ulid::new()));
        std::fs::create_dir(&root).expect("recovery dir");
        let state_file = root.join("flow.json");
        let queue_dir = root.join("queue");
        let token_config = TaskTokenConfig {
            hs256_secret: "test-task-token-secret-32-bytes!!".into(),
            issuer: Some("issuer".into()),
            audience: Some("audience".into()),
            ttl_seconds: 3_600,
        };
        let first_failure = Arc::new(AtomicBool::new(true));
        let worker_storage: Arc<Mutex<Option<ScopedStorage>>> = Arc::new(Mutex::new(None));
        let completed = Arc::new(AtomicUsize::new(0));
        let callback_errors = Arc::new(Mutex::new(Vec::new()));
        let worker = Router::new().route(
            "/dispatch",
            post({
                let first_failure = first_failure.clone();
                let worker_storage = worker_storage.clone();
                let completed = completed.clone();
                let callback_errors = callback_errors.clone();
                let token_config = token_config.clone();
                move |axum::Json(envelope): axum::Json<WorkerDispatchEnvelope>| {
                    let first_failure = first_failure.clone();
                    let worker_storage = worker_storage.clone();
                    let completed = completed.clone();
                    let callback_errors = callback_errors.clone();
                    let token_config = token_config.clone();
                    async move {
                        if first_failure.swap(false, Ordering::SeqCst) {
                            return StatusCode::SERVICE_UNAVAILABLE;
                        }
                        if decode_task_token(&token_config, &envelope.task_token).is_err() {
                            return StatusCode::UNAUTHORIZED;
                        }
                        let storage = worker_storage
                            .lock()
                            .expect("worker storage")
                            .clone()
                            .expect("reopened Flow storage");
                        let (_, fold) = MicroCompactor::new(storage.clone())
                            .load_state()
                            .await
                            .expect("callback projection");
                        let row = fold
                            .tasks
                            .get(&(envelope.run_id.clone(), envelope.task_key.clone()))
                            .expect("callback task");
                        let lookup = CurrentTask(CallbackTaskState {
                            state: "DISPATCHED".into(),
                            attempt: row.attempt,
                            attempt_id: row.attempt_id.clone().expect("active attempt"),
                            run_id: row.run_id.clone(),
                            task_key: row.task_key.clone(),
                            asset_key: row.asset_key.clone(),
                            partition_key: row.partition_key.clone(),
                            code_version: None,
                            cancel_requested: false,
                        });
                        let ctx = CallbackContext::new(
                            Arc::new(LedgerWriter::new(storage)),
                            Arc::new(TokenValidator(token_config)),
                            "tenant",
                            "workspace",
                        );
                        let result = handle_task_completed(
                            &ctx,
                            &callback_task_id(&envelope.run_id, &envelope.task_key),
                            &envelope.task_token,
                            TaskCompletedRequest {
                                attempt: envelope.attempt,
                                attempt_id: envelope.attempt_id,
                                worker_id: "operator-worker".into(),
                                traceparent: None,
                                outcome: WorkerOutcome::Succeeded,
                                completed_at: None,
                                output: None,
                                error: None,
                                metrics: None,
                                cancelled_during_phase: None,
                                partial_progress: None,
                            },
                            &lookup,
                        )
                        .await;
                        match result {
                            CallbackResult::Ok(_) => {
                                completed.fetch_add(1, Ordering::SeqCst);
                                StatusCode::NO_CONTENT
                            }
                            other => {
                                callback_errors
                                    .lock()
                                    .expect("callback errors")
                                    .push(format!("{other:?}"));
                                StatusCode::CONFLICT
                            }
                        }
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("worker listener");
        let target = format!(
            "http://{}/dispatch",
            listener.local_addr().expect("worker address")
        );
        let server = tokio::spawn(axum::serve(listener, worker).into_future());

        let backend: Arc<dyn StorageBackend> =
            Arc::new(PersistentFlowBackend::open(&state_file).expect("Flow backend"));
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scope");
        let ledger = LedgerWriter::new(storage.clone());
        let run_event = OrchestrationEvent::new(
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
        );
        let plan_event = OrchestrationEvent::new(
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
        );
        let mut dispatch = OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::DispatchRequested {
                run_id: "run".into(),
                task_key: "task".into(),
                attempt: 1,
                attempt_id: "attempt".into(),
                worker_queue: "default-queue".into(),
                dispatch_id: "dispatch:run:task:1".into(),
            },
        );
        dispatch.timestamp = chrono::Utc::now() - chrono::Duration::minutes(20);
        append_events_and_compact(&ledger, None, vec![run_event, plan_event, dispatch])
            .await
            .expect("seed durable Flow outbox");
        let queue = Arc::new(FileQueue::open(&queue_dir).expect("queue"));
        let lost_acceptance = Arc::new(LostFirstResponse {
            inner: queue.clone(),
            first: AtomicBool::new(true),
        });
        let dispatcher = DispatcherServiceState::new(
            storage.clone(),
            lost_acceptance.clone(),
            DispatcherServiceConfig {
                orch_compactor_url: None,
                worker_dispatch_headers: worker_dispatch_headers("local-secret").expect("headers"),
                dispatch_target_url: target.clone(),
                dispatch_target_audience: target.clone(),
                callback_base_url: "https://api.example".into(),
                task_token_config: token_config.clone(),
                task_timeout_secs: 1_800,
                timer_target_url: None,
                timer_target_audience: None,
                timer_queue: None,
            },
        )
        .expect("dispatcher");
        let response = router(dispatcher)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/run")
                    .body(Body::empty())
                    .expect("dispatch request"),
            )
            .await
            .expect("dispatch response");
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let (_, fold) = MicroCompactor::new(storage.clone())
            .load_state()
            .await
            .expect("initial fold");
        assert_eq!(
            fold.tasks
                .get(&("run".into(), "task".into()))
                .expect("task")
                .state,
            TaskState::Dispatched
        );
        assert_eq!(
            fold.dispatch_outbox.len(),
            1,
            "{:?}",
            fold.dispatch_outbox.keys()
        );
        assert!(
            fold.dispatch_outbox
                .get("dispatch:run:task:1")
                .expect("outbox")
                .created_at
                < chrono::Utc::now() - chrono::Duration::minutes(10),
            "{:?}",
            fold.dispatch_outbox.get("dispatch:run:task:1")
        );
        assert!(queue.drain_once(&reqwest::Client::new()).await.is_err());
        assert_eq!(completed.load(Ordering::SeqCst), 0);
        drop(lost_acceptance);
        drop(queue);
        drop(ledger);
        drop(storage);

        let child = tokio::process::Command::new(std::env::current_exe().expect("test binary"))
            .args(["--ignored", "--exact", "tests::recovery_reopen_child"])
            .env("ARCO_FLOW_RECOVERY_ROOT", &root)
            .output()
            .await
            .expect("restart observer");
        assert!(
            child.status.success(),
            "separate process could not reopen Flow state and queue: {}",
            String::from_utf8_lossy(&child.stdout)
        );

        // Reopen both authorities as a fresh process would. Replace the old
        // queued token with an already-expired signed token to model an outage.
        let backend: Arc<dyn StorageBackend> =
            Arc::new(PersistentFlowBackend::open(&state_file).expect("reopen Flow backend"));
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("reopen scope");
        let (_, fold) = MicroCompactor::new(storage.clone())
            .load_state()
            .await
            .expect("reopened fold");
        assert_eq!(
            fold.dispatch_outbox.len(),
            1,
            "{:?}",
            fold.dispatch_outbox.keys()
        );
        assert!(
            fold.dispatch_outbox
                .get("dispatch:run:task:1")
                .expect("reopened outbox")
                .created_at
                < chrono::Utc::now() - chrono::Duration::minutes(10)
        );
        *worker_storage.lock().expect("worker storage") = Some(storage.clone());
        let queue = Arc::new(FileQueue::open(&queue_dir).expect("reopen queue"));
        let expired = mint_task_token_for_attempt(
            &token_config,
            callback_task_id("run", "task"),
            "tenant",
            "workspace",
            "run",
            1,
            "attempt",
            chrono::Utc::now() - chrono::Duration::hours(2),
        )
        .expect("expired signed token");
        assert!(expired.expires_at < chrono::Utc::now());
        let pending = std::fs::read_dir(queue_dir.join("pending"))
            .expect("pending dir")
            .next()
            .expect("pending task")
            .expect("pending entry")
            .path();
        let mut record: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&pending).expect("queue record"))
                .expect("queue JSON");
        let body: Vec<u8> = serde_json::from_value(record["body"].clone()).expect("queue body");
        let mut envelope: serde_json::Value = serde_json::from_slice(&body).expect("envelope");
        envelope["taskToken"] = expired.token.into();
        envelope["tokenExpiresAt"] = serde_json::to_value(expired.expires_at).expect("expiry");
        record["body"] =
            serde_json::to_value(serde_json::to_vec(&envelope).expect("body")).expect("bytes");
        std::fs::write(&pending, serde_json::to_vec(&record).expect("record"))
            .expect("age queued task");
        std::fs::File::open(&pending)
            .expect("pending file")
            .sync_all()
            .expect("sync aged task");
        assert!(queue.drain_once(&reqwest::Client::new()).await.is_err());
        assert_eq!(completed.load(Ordering::SeqCst), 0);

        let ingress = Router::new().route(
            "/accept",
            post({
                let queue = queue.clone();
                move |axum::Json(task): axum::Json<serde_json::Value>| {
                    let queue = queue.clone();
                    async move {
                        let headers: HashMap<String, String> =
                            serde_json::from_value(task["headers"].clone()).expect("headers");
                        match queue
                            .enqueue_http(
                                task["taskId"].as_str().expect("task ID"),
                                task["targetUrl"].as_str().expect("target URL"),
                                task["body"].as_str().expect("worker body").as_bytes(),
                                EnqueueOptions::new(),
                                task["audience"].as_str(),
                                Some(headers),
                            )
                            .await
                            .expect("durable ingress")
                        {
                            EnqueueResult::Enqueued { .. } => StatusCode::ACCEPTED,
                            EnqueueResult::Deduplicated { .. } => StatusCode::CONFLICT,
                            EnqueueResult::QueueFull => StatusCode::TOO_MANY_REQUESTS,
                        }
                    }
                }
            }),
        );
        let ingress_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ingress listener");
        let ingress_url = format!(
            "http://{}/accept",
            ingress_listener.local_addr().expect("ingress address")
        );
        let ingress_server = tokio::spawn(axum::serve(ingress_listener, ingress).into_future());
        let sweeper = SweeperServiceState::new(
            storage.clone(),
            Arc::new(
                OperatorHttpEnqueuer::new(ingress_url, "ingress-secret".into())
                    .expect("HTTP transport"),
            ),
            SweeperServiceConfig {
                orch_compactor_url: None,
                worker_dispatch_headers: worker_dispatch_headers("local-secret").expect("headers"),
                dispatch_target_url: target.clone(),
                dispatch_target_audience: target,
                callback_base_url: "https://api.example".into(),
                task_token_config: token_config,
                task_timeout_secs: 1_800,
            },
        )
        .expect("operator sweeper");
        let response = sweeper_router(sweeper)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/run")
                    .body(Body::empty())
                    .expect("sweep request"),
            )
            .await
            .expect("sweep response");
        assert_eq!(response.status(), StatusCode::OK);
        let summary: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("summary"),
        )
        .expect("sweep JSON");
        assert_eq!(summary["redispatch_enqueued"], 1, "{summary}");
        assert_eq!(
            std::fs::read_dir(queue_dir.join("pending"))
                .expect("pending")
                .count(),
            2
        );
        assert!(queue.drain_once(&reqwest::Client::new()).await.is_err());
        assert_eq!(
            completed.load(Ordering::SeqCst),
            1,
            "{:?}",
            callback_errors.lock().expect("errors")
        );
        assert_eq!(
            std::fs::read_dir(queue_dir.join("pending"))
                .expect("pending")
                .count(),
            1,
            "the expired task stays pending for an operator retention policy"
        );

        let paths = storage
            .list(FlowPaths::ORCHESTRATION_LEDGER_PREFIX)
            .await
            .expect("ledger paths")
            .into_iter()
            .map(|path| path.as_str().to_string())
            .collect();
        MicroCompactor::new(storage)
            .compact_events(paths)
            .await
            .expect("fold callback");
        let backend: Arc<dyn StorageBackend> =
            Arc::new(PersistentFlowBackend::open(&state_file).expect("final reopen"));
        let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("final scope");
        let (_, fold) = MicroCompactor::new(storage)
            .load_state()
            .await
            .expect("final Flow state");
        assert_eq!(
            fold.tasks
                .get(&("run".into(), "task".into()))
                .expect("task")
                .state,
            TaskState::Succeeded
        );
        ingress_server.abort();
        server.abort();
        std::fs::remove_dir_all(root).expect("remove recovery dir");
    }

    #[tokio::test]
    async fn filesystem_queue_rejects_unsupported_routes_and_insecure_targets() {
        let root = std::env::temp_dir().join(format!("arco-operator-guard-{}", ulid::Ulid::new()));
        let queue = FileQueue::open(&root).expect("queue");
        assert!(
            queue
                .enqueue_http(
                    "task",
                    "http://worker.example/dispatch",
                    b"{}",
                    EnqueueOptions::new(),
                    None,
                    None
                )
                .await
                .is_err()
        );
        assert!(
            queue
                .enqueue_http(
                    "task",
                    "https://worker.example/dispatch",
                    b"{}",
                    EnqueueOptions::new().with_routing_key("priority"),
                    None,
                    None
                )
                .await
                .is_err()
        );
        assert_eq!(
            std::fs::read_dir(root.join("pending"))
                .expect("pending dir")
                .count(),
            0
        );
        std::fs::remove_dir_all(root).expect("remove test queue");
    }

    #[tokio::test]
    async fn accepted_task_survives_reopen_and_reaches_worker_once() {
        let root = std::env::temp_dir().join(format!("arco-operator-queue-{}", ulid::Ulid::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let received = seen.clone();
        let app = Router::new().route(
            "/dispatch",
            post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                let received = received.clone();
                async move {
                    received.lock().expect("worker lock").push(body);
                    StatusCode::NO_CONTENT
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let url = format!(
            "http://{}/dispatch",
            listener.local_addr().expect("listener address")
        );
        let server = tokio::spawn(axum::serve(listener, app).into_future());

        let queue = FileQueue::open(&root).expect("queue");
        let first = queue
            .enqueue_http(
                "task-1",
                &url,
                br#"{"dispatchId":"dispatch-1","taskToken":"token-a"}"#,
                EnqueueOptions::new(),
                Some(&url),
                Some(HashMap::new()),
            )
            .await
            .expect("durable accept");
        assert!(matches!(first, EnqueueResult::Enqueued { .. }));
        drop(queue); // Simulates losing the enqueue response and restarting the queue owner.

        let reopened = FileQueue::open(&root).expect("reopen queue");
        let second = reopened
            .enqueue_http(
                "task-1",
                &url,
                br#"{"dispatchId":"dispatch-1","taskToken":"token-b"}"#,
                EnqueueOptions::new(),
                Some(&url),
                Some(HashMap::new()),
            )
            .await
            .expect("deduplicate retry");
        assert!(matches!(second, EnqueueResult::Deduplicated { .. }));
        assert_eq!(
            reopened
                .drain_once(&reqwest::Client::new())
                .await
                .expect("delivery"),
            1
        );
        assert_eq!(
            reopened
                .drain_once(&reqwest::Client::new())
                .await
                .expect("empty drain"),
            0
        );
        assert_eq!(seen.lock().expect("worker lock").len(), 1);

        // A crash after publishing the done receipt can leave both directory
        // links. Reopening must finish cleanup without another worker call.
        let done = std::fs::read_dir(root.join("done"))
            .expect("done directory")
            .next()
            .expect("done record")
            .expect("done entry")
            .path();
        let pending = root
            .join("pending")
            .join(done.file_name().expect("done name"));
        std::fs::hard_link(&done, &pending).expect("simulate interrupted cleanup");
        let reopened = FileQueue::open(&root).expect("reopen after interrupted cleanup");
        assert_eq!(
            reopened
                .drain_once(&reqwest::Client::new())
                .await
                .expect("recover terminal receipt"),
            0
        );
        assert_eq!(seen.lock().expect("worker lock").len(), 1);
        server.abort();
        std::fs::remove_dir_all(root).expect("remove test queue");
    }

    #[tokio::test]
    async fn dispatcher_publishes_to_disk_then_reopened_queue_delivers() {
        let root = std::env::temp_dir().join(format!("arco-operator-flow-{}", ulid::Ulid::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let received = seen.clone();
        let worker = Router::new().route(
            "/dispatch",
            post(
                move |headers: HeaderMap, axum::Json(body): axum::Json<serde_json::Value>| {
                    let received = received.clone();
                    async move {
                        if headers
                            .get("X-Arco-Dispatch-Secret")
                            .and_then(|v| v.to_str().ok())
                            != Some("local-secret")
                        {
                            return StatusCode::FORBIDDEN;
                        }
                        received.lock().expect("worker lock").push(body);
                        StatusCode::NO_CONTENT
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("worker listener");
        let target = format!(
            "http://{}/dispatch",
            listener.local_addr().expect("worker address")
        );
        let server = tokio::spawn(axum::serve(listener, worker).into_future());

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
                            partition_key: Some("day=1".into()),
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
        let durable_queue = Arc::new(FileQueue::open(&root).expect("queue"));
        let lose_first_response = Arc::new(AtomicBool::new(true));
        let ingress = Router::new().route(
            "/accept",
            post({
                let durable_queue = durable_queue.clone();
                let lose_first_response = lose_first_response.clone();
                move |headers: HeaderMap, axum::Json(task): axum::Json<serde_json::Value>| {
                    let durable_queue = durable_queue.clone();
                    let lose_first_response = lose_first_response.clone();
                    async move {
                        if headers.get("authorization").and_then(|v| v.to_str().ok())
                            != Some("Bearer ingress-secret")
                        {
                            return StatusCode::FORBIDDEN;
                        }
                        let id = task["taskId"].as_str().expect("task ID");
                        let target = task["targetUrl"].as_str().expect("target URL");
                        let body = task["body"].as_str().expect("worker body");
                        let audience = task["audience"].as_str();
                        let headers: HashMap<String, String> =
                            serde_json::from_value(task["headers"].clone()).expect("headers");
                        let accepted = durable_queue
                            .enqueue_http(
                                id,
                                target,
                                body.as_bytes(),
                                EnqueueOptions::new(),
                                audience,
                                Some(headers),
                            )
                            .await
                            .expect("durable ingress");
                        if lose_first_response.swap(false, Ordering::SeqCst) {
                            return StatusCode::INTERNAL_SERVER_ERROR;
                        }
                        match accepted {
                            EnqueueResult::Enqueued { .. } => StatusCode::ACCEPTED,
                            EnqueueResult::Deduplicated { .. } => StatusCode::CONFLICT,
                            EnqueueResult::QueueFull => StatusCode::TOO_MANY_REQUESTS,
                        }
                    }
                }
            }),
        );
        let ingress_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ingress listener");
        let ingress_url = format!(
            "http://{}/accept",
            ingress_listener.local_addr().expect("ingress address")
        );
        let ingress_server = tokio::spawn(axum::serve(ingress_listener, ingress).into_future());
        let queue = Arc::new(
            OperatorHttpEnqueuer::new(ingress_url, "ingress-secret".into())
                .expect("HTTP transport"),
        );
        let state = DispatcherServiceState::new(
            storage,
            queue.clone(),
            DispatcherServiceConfig {
                orch_compactor_url: None,
                worker_dispatch_headers: worker_dispatch_headers("local-secret").expect("headers"),
                dispatch_target_url: target.clone(),
                dispatch_target_audience: target,
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
            },
        )
        .expect("workspace dispatcher");
        let app = router(state);
        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/run")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("dispatcher response");
        assert_eq!(first.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let second = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/run")
                    .body(Body::empty())
                    .expect("retry request"),
            )
            .await
            .expect("retry response");
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(seen.lock().expect("worker lock").len(), 0);
        drop(queue);

        let reopened = FileQueue::open(&root).expect("reopen queue");
        assert_eq!(
            reopened
                .drain_once(&reqwest::Client::new())
                .await
                .expect("deliver"),
            1
        );
        let delivered = seen.lock().expect("worker lock");
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0]["dispatchId"], "dispatch");
        assert_eq!(delivered[0]["partitionKey"], "day=1");
        drop(delivered);
        ingress_server.abort();
        server.abort();
        std::fs::remove_dir_all(root).expect("remove test queue");
    }

    #[tokio::test]
    async fn worker_failure_keeps_task_for_retry_after_reopen() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let root = std::env::temp_dir().join(format!("arco-operator-retry-{}", ulid::Ulid::new()));
        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let worker = Router::new().route(
            "/dispatch",
            post(move || {
                let counter = counter.clone();
                async move {
                    if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                        StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        StatusCode::NO_CONTENT
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("worker listener");
        let url = format!(
            "http://{}/dispatch",
            listener.local_addr().expect("worker address")
        );
        let server = tokio::spawn(axum::serve(listener, worker).into_future());
        let queue = FileQueue::open(&root).expect("queue");
        queue
            .enqueue_http(
                "retry-task",
                &url,
                br#"{"dispatchId":"retry"}"#,
                EnqueueOptions::new(),
                None,
                None,
            )
            .await
            .expect("enqueue");
        queue
            .enqueue_http(
                "healthy",
                &url,
                br#"{"dispatchId":"healthy"}"#,
                EnqueueOptions::new(),
                None,
                None,
            )
            .await
            .expect("enqueue healthy task");
        assert!(queue.drain_once(&reqwest::Client::new()).await.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            std::fs::read_dir(root.join("pending"))
                .expect("pending")
                .count(),
            1
        );
        drop(queue);
        let reopened = FileQueue::open(&root).expect("reopen queue");
        assert_eq!(
            reopened
                .drain_once(&reqwest::Client::new())
                .await
                .expect("retry"),
            1
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        server.abort();
        std::fs::remove_dir_all(root).expect("remove test queue");
    }
}
