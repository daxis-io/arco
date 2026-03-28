//! Arco Flow orchestration dispatcher service.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;

use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{
    DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS, DEFAULT_TASK_TOKEN_TTL_SECONDS, ScopedStorage,
    TaskTokenConfig, mint_task_token_for_run,
};
use arco_flow::dispatch::cloud_tasks::{CloudTasksConfig, CloudTasksDispatcher};
use arco_flow::dispatch::{EnqueueOptions, EnqueueResult};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::controllers::{
    DispatchAction, DispatcherController, ReadyDispatchController, TimerAction, TimerController,
};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData};
use arco_flow::orchestration::worker_contract::WorkerDispatchEnvelope;

#[derive(Clone)]
struct AppState {
    tenant_id: String,
    workspace_id: String,
    compactor: MicroCompactor,
    ledger: LedgerWriter,
    cloud_tasks: Arc<CloudTasksDispatcher>,
    dispatch_target_url: String,
    callback_base_url: String,
    task_token_config: TaskTokenConfig,
    timer_target_url: Option<String>,
    timer_queue: Option<String>,
}

#[derive(Debug, Serialize)]
struct RunError {
    kind: String,
    id: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    ready_dispatch_emitted: usize,
    ready_dispatch_skipped: usize,
    dispatch_actions: usize,
    dispatch_enqueued: usize,
    dispatch_deduplicated: usize,
    dispatch_failed: usize,
    timer_actions: usize,
    timer_enqueued: usize,
    timer_deduplicated: usize,
    timer_failed: usize,
    errors: Vec<RunError>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
struct ApiError {
    message: String,
    summary: Option<RunSummary>,
}

impl ApiError {
    fn from_summary(summary: RunSummary) -> Self {
        Self {
            message: "dispatcher run completed with errors".to_string(),
            summary: Some(summary),
        }
    }
}

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self {
            message: error.to_string(),
            summary: None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = StatusCode::INTERNAL_SERVER_ERROR;
        if let Some(summary) = self.summary {
            return (status, Json(summary)).into_response();
        }

        (
            status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
            .into_response()
    }
}

#[derive(Debug, Serialize)]
struct TimerPayload {
    timer_id: String,
    run_id: Option<String>,
    task_key: Option<String>,
    attempt: Option<u32>,
    fire_at: DateTime<Utc>,
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn append_and_compact_events(
    ledger: &LedgerWriter,
    compactor: &MicroCompactor,
    events: Vec<OrchestrationEvent>,
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let event_paths = events.iter().map(LedgerWriter::event_path).collect();
    ledger.append_all(events).await?;
    compactor.compact_events(event_paths).await?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_handler(
    State(state): State<AppState>,
) -> std::result::Result<Json<RunSummary>, ApiError> {
    let (manifest, fold_state) = state.compactor.load_state().await?;

    let ready_controller = ReadyDispatchController::with_defaults();
    let ready_actions = ready_controller.reconcile(&manifest, &fold_state);

    let mut ready_events = Vec::new();
    let mut ready_emitted = 0;
    let mut ready_skipped = 0;

    for action in ready_actions {
        match action.into_event_data() {
            Some(data) => {
                ready_emitted += 1;
                ready_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    data,
                ));
            }
            None => ready_skipped += 1,
        }
    }

    append_and_compact_events(&state.ledger, &state.compactor, ready_events).await?;

    let (manifest, fold_state) = state.compactor.load_state().await?;

    let outbox_rows: Vec<_> = fold_state.dispatch_outbox.values().cloned().collect();
    let dispatcher = DispatcherController::with_defaults();
    let dispatch_actions = dispatcher.reconcile(&manifest, &outbox_rows);

    let mut dispatch_events = Vec::new();
    let mut dispatch_enqueued = 0;
    let mut dispatch_deduplicated = 0;
    let mut dispatch_failed = 0;
    let mut errors = Vec::new();

    for action in &dispatch_actions {
        let DispatchAction::CreateCloudTask {
            dispatch_id,
            cloud_task_id,
            run_id,
            task_key,
            attempt,
            attempt_id,
            worker_queue,
        } = action
        else {
            continue;
        };

        let minted = mint_task_token_for_run(
            &state.task_token_config,
            task_key.clone(),
            state.tenant_id.clone(),
            state.workspace_id.clone(),
            Some(run_id.clone()),
            Utc::now(),
        )
        .map_err(|e| Error::configuration(format!("task token minting failed: {e}")))?;

        let envelope = WorkerDispatchEnvelope {
            tenant_id: state.tenant_id.clone(),
            workspace_id: state.workspace_id.clone(),
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            attempt_id: attempt_id.clone(),
            dispatch_id: dispatch_id.clone(),
            worker_queue: worker_queue.clone(),
            callback_base_url: state.callback_base_url.clone(),
            task_token: minted.token,
            token_expires_at: minted.expires_at,
            traceparent: None,
            payload: serde_json::Value::Object(serde_json::Map::new()),
        };
        let body = envelope
            .to_json()
            .map_err(|e| Error::serialization(format!("dispatch envelope error: {e}")))?;

        let mut options = EnqueueOptions::new();
        if worker_queue != "default-queue" {
            options = options.with_routing_key(worker_queue.clone());
        }

        let result = state
            .cloud_tasks
            .enqueue_http(
                cloud_task_id,
                &state.dispatch_target_url,
                body.as_bytes(),
                options,
                Some(state.dispatch_target_url.as_str()),
                None,
            )
            .await;

        match result {
            Ok(EnqueueResult::Enqueued { .. }) => {
                dispatch_enqueued += 1;
                dispatch_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::DispatchEnqueued {
                        dispatch_id: dispatch_id.clone(),
                        run_id: Some(run_id.clone()),
                        task_key: Some(task_key.clone()),
                        attempt: Some(*attempt),
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::Deduplicated { .. }) => {
                dispatch_deduplicated += 1;
                dispatch_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::DispatchEnqueued {
                        dispatch_id: dispatch_id.clone(),
                        run_id: Some(run_id.clone()),
                        task_key: Some(task_key.clone()),
                        attempt: Some(*attempt),
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::QueueFull) => {
                dispatch_failed += 1;
                errors.push(RunError {
                    kind: "dispatch_queue_full".to_string(),
                    id: dispatch_id.clone(),
                    message: "queue full".to_string(),
                });
            }
            Err(err) => {
                dispatch_failed += 1;
                errors.push(RunError {
                    kind: "dispatch_enqueue_failed".to_string(),
                    id: dispatch_id.clone(),
                    message: err.to_string(),
                });
            }
        }
    }

    append_and_compact_events(&state.ledger, &state.compactor, dispatch_events).await?;

    let timer_rows: Vec<_> = fold_state.timers.values().cloned().collect();
    let timer_controller = TimerController::with_defaults();
    let timer_actions = timer_controller.reconcile(&manifest, &timer_rows);

    let mut timer_events = Vec::new();
    let mut timer_enqueued = 0;
    let mut timer_deduplicated = 0;
    let mut timer_failed = 0;

    if !timer_actions.is_empty() && state.timer_target_url.is_none() {
        errors.push(RunError {
            kind: "timer_target_missing".to_string(),
            id: "timers".to_string(),
            message: "ARCO_FLOW_TIMER_TARGET_URL is not set".to_string(),
        });
    }

    for action in &timer_actions {
        let TimerAction::CreateTimer {
            timer_id,
            cloud_task_id,
            fire_at,
            run_id,
            task_key,
            attempt,
            ..
        } = action
        else {
            continue;
        };

        let Some(target_url) = state.timer_target_url.as_ref() else {
            timer_failed += 1;
            continue;
        };

        let payload = TimerPayload {
            timer_id: timer_id.clone(),
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            fire_at: *fire_at,
        };
        let body = serde_json::to_vec(&payload)
            .map_err(|e| Error::serialization(format!("timer payload error: {e}")))?;

        let now = Utc::now();
        let delay = fire_at.signed_duration_since(now).to_std().ok();

        let mut options = EnqueueOptions::new();
        if let Some(delay) = delay {
            options = options.with_delay(delay);
        }
        if let Some(queue) = state.timer_queue.as_ref() {
            options = options.with_routing_key(queue.clone());
        }

        let result = state
            .cloud_tasks
            .enqueue_http(
                cloud_task_id,
                target_url,
                &body,
                options,
                Some(target_url.as_str()),
                None,
            )
            .await;

        match result {
            Ok(EnqueueResult::Enqueued { .. }) => {
                timer_enqueued += 1;
                timer_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::TimerEnqueued {
                        timer_id: timer_id.clone(),
                        run_id: run_id.clone(),
                        task_key: task_key.clone(),
                        attempt: *attempt,
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::Deduplicated { .. }) => {
                timer_deduplicated += 1;
                timer_events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::TimerEnqueued {
                        timer_id: timer_id.clone(),
                        run_id: run_id.clone(),
                        task_key: task_key.clone(),
                        attempt: *attempt,
                        cloud_task_id: cloud_task_id.clone(),
                    },
                ));
            }
            Ok(EnqueueResult::QueueFull) => {
                timer_failed += 1;
                errors.push(RunError {
                    kind: "timer_queue_full".to_string(),
                    id: timer_id.clone(),
                    message: "queue full".to_string(),
                });
            }
            Err(err) => {
                timer_failed += 1;
                errors.push(RunError {
                    kind: "timer_enqueue_failed".to_string(),
                    id: timer_id.clone(),
                    message: err.to_string(),
                });
            }
        }
    }

    append_and_compact_events(&state.ledger, &state.compactor, timer_events).await?;

    let summary = RunSummary {
        ready_dispatch_emitted: ready_emitted,
        ready_dispatch_skipped: ready_skipped,
        dispatch_actions: dispatch_actions.len(),
        dispatch_enqueued,
        dispatch_deduplicated,
        dispatch_failed,
        timer_actions: timer_actions.len(),
        timer_enqueued,
        timer_deduplicated,
        timer_failed,
        errors,
    };

    if summary.errors.is_empty() {
        Ok(Json(summary))
    } else {
        Err(ApiError::from_summary(summary))
    }
}

fn required_env(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| Error::configuration(format!("missing {key}")))
}

fn optional_env(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn parse_bool_env(key: &str, default: bool) -> bool {
    std::env::var(key).map_or(default, |value| value.eq_ignore_ascii_case("true"))
}

fn parse_u64_env(key: &str, default: u64) -> Result<u64> {
    parse_u64_value(optional_env(key).as_deref(), key, default)
}

fn parse_u64_value(raw: Option<&str>, key: &str, default: u64) -> Result<u64> {
    let Some(raw) = raw else {
        return Ok(default);
    };

    raw.parse::<u64>()
        .map_err(|_| Error::configuration(format!("invalid {key}")))
}

fn task_token_config_from_env(task_timeout_secs: u64) -> Result<TaskTokenConfig> {
    task_token_config_from_parts(
        required_env("ARCO_FLOW_TASK_TOKEN_SECRET")?,
        optional_env("ARCO_FLOW_TASK_TOKEN_ISSUER"),
        optional_env("ARCO_FLOW_TASK_TOKEN_AUDIENCE"),
        parse_u64_env(
            "ARCO_FLOW_TASK_TOKEN_TTL_SECS",
            DEFAULT_TASK_TOKEN_TTL_SECONDS,
        )?,
        task_timeout_secs,
    )
}

fn task_token_config_from_parts(
    hs256_secret: String,
    issuer: Option<String>,
    audience: Option<String>,
    ttl_seconds: u64,
    task_timeout_secs: u64,
) -> Result<TaskTokenConfig> {
    let config = TaskTokenConfig {
        hs256_secret,
        issuer,
        audience,
        ttl_seconds,
    };
    config
        .validate_for_dispatch(task_timeout_secs, true)
        .map_err(|e| Error::configuration(e.to_string()))?;
    Ok(config)
}

fn task_timeout_seconds_from_env() -> Result<u64> {
    let timeout = parse_u64_env(
        "ARCO_FLOW_TASK_TIMEOUT_SECS",
        DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS,
    )?;
    validate_task_timeout_seconds(timeout)
}

fn validate_task_timeout_seconds(timeout: u64) -> Result<u64> {
    if timeout == 0 {
        return Err(Error::configuration(
            "ARCO_FLOW_TASK_TIMEOUT_SECS must be greater than zero",
        ));
    }
    Ok(timeout)
}

fn resolve_port() -> Result<u16> {
    if let Ok(port) = std::env::var("PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid PORT"));
    }

    if let Ok(port) = std::env::var("ARCO_FLOW_PORT") {
        return port
            .parse::<u16>()
            .map_err(|_| Error::configuration("invalid ARCO_FLOW_PORT"));
    }

    Ok(8080)
}

fn log_format_from_env() -> LogFormat {
    match std::env::var("ARCO_LOG_FORMAT") {
        Ok(value) if value.eq_ignore_ascii_case("json") => LogFormat::Json,
        _ => LogFormat::Pretty,
    }
}

#[allow(clippy::unused_async)]
async fn build_cloud_tasks(config: CloudTasksConfig) -> Result<CloudTasksDispatcher> {
    #[cfg(feature = "gcp")]
    {
        CloudTasksDispatcher::new(config).await
    }

    #[cfg(not(feature = "gcp"))]
    {
        CloudTasksDispatcher::new(config)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging(log_format_from_env());

    let tenant_id = required_env("ARCO_TENANT_ID")?;
    let workspace_id = required_env("ARCO_WORKSPACE_ID")?;
    let bucket = required_env("ARCO_STORAGE_BUCKET")?;
    let dispatch_target_url = required_env("ARCO_FLOW_DISPATCH_TARGET_URL")?;
    let callback_base_url = required_env("ARCO_FLOW_CALLBACK_BASE_URL")?;
    let project_id = required_env("ARCO_GCP_PROJECT_ID")?;
    let location = required_env("ARCO_GCP_LOCATION")?;
    let queue_name =
        optional_env("ARCO_FLOW_QUEUE").unwrap_or_else(|| "arco-flow-dispatch".to_string());
    let timer_target_url = optional_env("ARCO_FLOW_TIMER_TARGET_URL");
    let timer_queue = optional_env("ARCO_FLOW_TIMER_QUEUE");
    let service_account_email = optional_env("ARCO_FLOW_SERVICE_ACCOUNT_EMAIL");
    let task_timeout_secs = task_timeout_seconds_from_env()?;
    let task_token_config = task_token_config_from_env(task_timeout_secs)?;
    let port = resolve_port()?;

    let mut cloud_config = CloudTasksConfig::new(
        project_id,
        location,
        queue_name,
        dispatch_target_url.clone(),
    );

    if let Some(email) = service_account_email {
        cloud_config = cloud_config.with_service_account(email);
    }

    let apply_queue_updates = parse_bool_env("ARCO_FLOW_APPLY_QUEUE_RETRY_CONFIG", false);
    if !apply_queue_updates {
        cloud_config = cloud_config.with_queue_retry_updates(false);
    }

    cloud_config =
        cloud_config.with_task_timeout(std::time::Duration::from_secs(task_timeout_secs));

    let cloud_tasks = build_cloud_tasks(cloud_config).await?;

    let backend = ObjectStoreBackend::from_bucket(&bucket)?;
    let backend: Arc<dyn StorageBackend> = Arc::new(backend);
    let storage = ScopedStorage::new(backend, tenant_id.clone(), workspace_id.clone())?;

    let state = AppState {
        tenant_id,
        workspace_id,
        compactor: MicroCompactor::new(storage.clone()),
        ledger: LedgerWriter::new(storage),
        cloud_tasks: Arc::new(cloud_tasks),
        dispatch_target_url,
        callback_base_url,
        task_token_config,
        timer_target_url,
        timer_queue,
    };

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/run", post(run_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| Error::configuration(format!("failed to bind: {e}")))?;

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::configuration(format!("server error: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arco_core::{MemoryBackend, ScopedStorage};
    use arco_flow::orchestration::compactor::fold::DispatchStatus;
    use arco_flow::orchestration::controllers::{
        DispatchAction, DispatcherController, ReadyDispatchAction, ReadyDispatchController,
    };
    use arco_flow::orchestration::events::{TaskDef, TriggerInfo};

    #[test]
    fn task_token_config_from_parts_rejects_missing_issuer() {
        let err = task_token_config_from_parts(
            "secret".to_string(),
            None,
            Some("audience".to_string()),
            3_600,
            1_800,
        )
        .expect_err("missing issuer must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }

    #[test]
    fn task_token_config_from_parts_rejects_missing_audience() {
        let err = task_token_config_from_parts(
            "secret".to_string(),
            Some("issuer".to_string()),
            None,
            3_600,
            1_800,
        )
        .expect_err("missing audience must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }

    #[test]
    fn parse_u64_value_rejects_invalid_timeout_env() {
        let err = parse_u64_value(
            Some("not-a-number"),
            "ARCO_FLOW_TASK_TIMEOUT_SECS",
            DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS,
        )
        .expect_err("invalid timeout must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }

    #[test]
    fn validate_task_timeout_seconds_rejects_zero() {
        let err = validate_task_timeout_seconds(0).expect_err("zero timeout must fail");
        assert!(matches!(err, Error::Configuration { .. }));
    }

    #[tokio::test]
    async fn append_and_compact_events_refreshes_projection_for_dispatch_requested() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let ledger = LedgerWriter::new(storage.clone());
        let compactor = MicroCompactor::new(storage);

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
        let initial_paths = vec![
            LedgerWriter::event_path(&run_triggered),
            LedgerWriter::event_path(&plan_created),
        ];
        ledger.append(run_triggered).await?;
        ledger.append(plan_created).await?;
        compactor.compact_events(initial_paths).await?;

        let (manifest, fold_state) = compactor.load_state().await?;
        let ready_actions =
            ReadyDispatchController::with_defaults().reconcile(&manifest, &fold_state);
        assert_eq!(ready_actions.len(), 1);

        let ready_events: Vec<_> = ready_actions
            .into_iter()
            .filter_map(ReadyDispatchAction::into_event_data)
            .map(|data| OrchestrationEvent::new("tenant", "workspace", data))
            .collect();

        append_and_compact_events(&ledger, &compactor, ready_events).await?;

        let (manifest, fold_state) = compactor.load_state().await?;
        let dispatch_id = format!("dispatch:{run_id}:{task_key}:1");
        let outbox_row = fold_state
            .dispatch_outbox
            .get(&dispatch_id)
            .expect("dispatch requested must be visible after compaction");
        assert_eq!(outbox_row.status, DispatchStatus::Pending);

        let dispatch_actions = DispatcherController::with_defaults().reconcile(
            &manifest,
            &fold_state
                .dispatch_outbox
                .values()
                .cloned()
                .collect::<Vec<_>>(),
        );
        assert_eq!(dispatch_actions.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn append_and_compact_events_refreshes_projection_for_dispatch_enqueued() -> Result<()> {
        let backend = Arc::new(MemoryBackend::new());
        let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
        let ledger = LedgerWriter::new(storage.clone());
        let compactor = MicroCompactor::new(storage);

        let dispatch_requested = OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::DispatchRequested {
                run_id: "run_01".to_string(),
                task_key: "extract".to_string(),
                attempt: 1,
                attempt_id: "attempt_01".to_string(),
                worker_queue: "default-queue".to_string(),
                dispatch_id: "dispatch:run_01:extract:1".to_string(),
            },
        );
        append_and_compact_events(&ledger, &compactor, vec![dispatch_requested]).await?;

        let dispatch_action = DispatcherController::with_defaults()
            .reconcile(
                &compactor.load_state().await?.0,
                &compactor
                    .load_state()
                    .await?
                    .1
                    .dispatch_outbox
                    .values()
                    .cloned()
                    .collect::<Vec<_>>(),
            )
            .into_iter()
            .next()
            .expect("dispatch action");

        let DispatchAction::CreateCloudTask {
            cloud_task_id,
            dispatch_id,
            ..
        } = dispatch_action
        else {
            panic!("expected CreateCloudTask action");
        };

        let dispatch_enqueued = OrchestrationEvent::new(
            "tenant",
            "workspace",
            OrchestrationEventData::DispatchEnqueued {
                dispatch_id: dispatch_id.clone(),
                run_id: Some("run_01".to_string()),
                task_key: Some("extract".to_string()),
                attempt: Some(1),
                cloud_task_id: cloud_task_id.clone(),
            },
        );
        append_and_compact_events(&ledger, &compactor, vec![dispatch_enqueued]).await?;

        let (_, fold_state) = compactor.load_state().await?;
        let outbox_row = fold_state
            .dispatch_outbox
            .get(&dispatch_id)
            .expect("dispatch enqueued must be visible after compaction");
        assert_eq!(outbox_row.status, DispatchStatus::Created);
        assert_eq!(
            outbox_row.cloud_task_id.as_deref(),
            Some(cloud_task_id.as_str())
        );

        Ok(())
    }
}
