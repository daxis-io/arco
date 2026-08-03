//! Arco Flow orchestration anti-entropy sweeper service.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Serialize;
use ulid::Ulid;

use arco_core::observability::{LogFormat, init_logging};
use arco_core::storage::{ObjectStoreBackend, StorageBackend};
use arco_core::{
    DEFAULT_DISPATCH_TASK_TIMEOUT_SECONDS, DEFAULT_TASK_TOKEN_TTL_SECONDS, ScopedStorage,
    TaskTokenConfig, mint_task_token_for_attempt,
};
use arco_flow::dispatch::cloud_tasks::{
    CloudTasksConfig, CloudTasksDispatcher, resolve_target_audience,
};
use arco_flow::dispatch::worker_auth::worker_dispatch_headers;
use arco_flow::dispatch::{EnqueueOptions, EnqueueResult};
use arco_flow::error::{Error, Result};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::compactor::fold::DispatchOutboxRow;
use arco_flow::orchestration::controllers::{AntiEntropySweeper, Repair};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TaskOutcome};
use arco_flow::orchestration::flow_service::{
    append_events_and_compact, orchestration_ledger_freshness,
};
use arco_flow::orchestration::ids::{cloud_task_id, deterministic_attempt_id};
use arco_flow::orchestration::worker_contract::{
    DispatchEnvelopeSpec, dispatch_envelope_for_attempt,
};
use arco_worker_contract::callback_task_id;

#[async_trait::async_trait]
trait CloudTaskEnqueuer: Send + Sync {
    async fn enqueue_http(
        &self,
        task_id: &str,
        target_url: &str,
        body: &[u8],
        options: EnqueueOptions,
        audience: Option<&str>,
        extra_headers: Option<HashMap<String, String>>,
    ) -> Result<EnqueueResult>;
}

#[async_trait::async_trait]
impl CloudTaskEnqueuer for CloudTasksDispatcher {
    async fn enqueue_http(
        &self,
        task_id: &str,
        target_url: &str,
        body: &[u8],
        options: EnqueueOptions,
        audience: Option<&str>,
        extra_headers: Option<HashMap<String, String>>,
    ) -> Result<EnqueueResult> {
        Self::enqueue_http(
            self,
            task_id,
            target_url,
            body,
            options,
            audience,
            extra_headers,
        )
        .await
    }
}

#[derive(Clone)]
struct AppState {
    tenant_id: String,
    workspace_id: String,
    storage: ScopedStorage,
    compactor: MicroCompactor,
    ledger: LedgerWriter,
    orch_compactor_url: Option<String>,
    cloud_tasks: Arc<dyn CloudTaskEnqueuer>,
    worker_dispatch_headers: HashMap<String, String>,
    dispatch_target_url: String,
    dispatch_target_audience: String,
    callback_base_url: String,
    task_token_config: TaskTokenConfig,
    clock: SweeperClock,
}

/// Time source for sweeper decisions.
///
/// Production reads the system clock. Tests pin it so a durable retry deadline
/// can be crossed without sleeping, which is what lets the wiring test drive
/// the real `/run` route rather than the pure controller.
#[derive(Clone, Copy)]
struct SweeperClock {
    fixed: Option<chrono::DateTime<Utc>>,
}

impl SweeperClock {
    const fn system() -> Self {
        Self { fixed: None }
    }

    fn now(self) -> chrono::DateTime<Utc> {
        self.fixed.unwrap_or_else(Utc::now)
    }
}

#[derive(Debug, Serialize)]
struct RunError {
    kind: String,
    id: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    repairs_created: usize,
    redispatch_attempted: usize,
    redispatch_enqueued: usize,
    redispatch_deduplicated: usize,
    redispatch_failed: usize,
    skipped_due_to_lag: usize,
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
            message: "sweeper run completed with errors".to_string(),
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

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

/// Builds the sweeper's route table.
fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/run", post(run_handler))
        .with_state(state)
}

#[allow(clippy::too_many_lines)]
async fn run_handler(
    State(state): State<AppState>,
) -> std::result::Result<Json<RunSummary>, ApiError> {
    let (manifest, fold_state) = state.compactor.load_state().await?;
    let sweeper = AntiEntropySweeper::with_defaults();

    let tasks: Vec<_> = fold_state.tasks.values().cloned().collect();
    let outbox: Vec<_> = fold_state.dispatch_outbox.values().cloned().collect();
    let outbox_by_id: HashMap<String, DispatchOutboxRow> = outbox
        .iter()
        .cloned()
        .map(|row| (row.dispatch_id.clone(), row))
        .collect();

    let now = state.clock.now();
    // Check ledger freshness so an idle workspace (no event flow keeping the
    // wall-clock watermark fresh) can still reap zombie tasks; see issue #338.
    let ledger_freshness =
        orchestration_ledger_freshness(&state.storage, &manifest.watermarks, now).await;
    let repairs = sweeper.scan_with_ledger_freshness(
        &manifest.watermarks,
        ledger_freshness,
        &tasks,
        &outbox,
        now,
    );

    let mut events = Vec::new();
    let mut errors = Vec::new();
    let mut repairs_created = 0;
    let mut redispatch_attempted = 0;
    let mut redispatch_enqueued = 0;
    let mut redispatch_deduplicated = 0;
    let mut redispatch_failed = 0;
    let mut skipped_due_to_lag = 0;

    for repair in repairs {
        match repair {
            Repair::CreateDispatchOutbox {
                run_id,
                task_key,
                attempt,
                ..
            } => {
                let dispatch_id = DispatchOutboxRow::dispatch_id(&run_id, &task_key, attempt);
                let attempt_id = deterministic_attempt_id(&dispatch_id);

                events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::DispatchRequested {
                        run_id,
                        task_key,
                        attempt,
                        attempt_id,
                        worker_queue: "default-queue".to_string(),
                        dispatch_id,
                    },
                ));

                repairs_created += 1;
            }
            Repair::RedispatchStuckTask {
                run_id,
                task_key,
                attempt,
                original_dispatch_id,
                ..
            } => {
                redispatch_attempted += 1;

                let attempt_id = outbox_by_id
                    .get(&original_dispatch_id)
                    .map(|row| row.attempt_id.clone())
                    .filter(|id| !id.is_empty())
                    .unwrap_or_else(|| deterministic_attempt_id(&original_dispatch_id));

                let callback_task_id = callback_task_id(&run_id, &task_key);
                let minted = mint_task_token_for_attempt(
                    &state.task_token_config,
                    callback_task_id.clone(),
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    run_id.clone(),
                    attempt,
                    attempt_id.clone(),
                    Utc::now(),
                )
                .map_err(|e| Error::configuration(format!("task token minting failed: {e}")))?;

                let task_row = fold_state
                    .tasks
                    .get(&(run_id.clone(), task_key.clone()))
                    .ok_or_else(|| {
                        Error::dispatch(format!(
                            "refusing unscoped redispatch for missing task row: run={run_id} task={task_key}"
                        ))
                    })?;
                let envelope = dispatch_envelope_for_attempt(
                    DispatchEnvelopeSpec {
                        tenant_id: state.tenant_id.clone(),
                        workspace_id: state.workspace_id.clone(),
                        run_id: run_id.clone(),
                        task_key: task_key.clone(),
                        attempt,
                        attempt_id,
                        dispatch_id: original_dispatch_id.clone(),
                        worker_queue: "default-queue".to_string(),
                        callback_base_url: state.callback_base_url.clone(),
                        task_token: minted.token,
                        token_expires_at: minted.expires_at,
                    },
                    Some(task_row),
                );

                let body = envelope
                    .to_json()
                    .map_err(|e| Error::serialization(format!("dispatch envelope error: {e}")))?;

                let repair_epoch = outbox_by_id
                    .get(&original_dispatch_id)
                    .map_or("missing_dispatch_outbox", |row| row.row_version.as_str());
                let repair_attempt_id = Ulid::new().to_string();
                let cloud_id = redispatch_cloud_task_id(
                    &original_dispatch_id,
                    repair_epoch,
                    &repair_attempt_id,
                );
                let options = EnqueueOptions::new();

                let result = state
                    .cloud_tasks
                    .enqueue_http(
                        &cloud_id,
                        &state.dispatch_target_url,
                        body.as_bytes(),
                        options,
                        Some(state.dispatch_target_audience.as_str()),
                        Some(state.worker_dispatch_headers.clone()),
                    )
                    .await;

                match result {
                    Ok(EnqueueResult::Enqueued { .. }) => {
                        redispatch_enqueued += 1;
                        events.push(OrchestrationEvent::new(
                            state.tenant_id.clone(),
                            state.workspace_id.clone(),
                            OrchestrationEventData::DispatchEnqueued {
                                dispatch_id: original_dispatch_id.clone(),
                                run_id: Some(run_id),
                                task_key: Some(task_key),
                                attempt: Some(attempt),
                                cloud_task_id: cloud_id,
                            },
                        ));
                    }
                    Ok(EnqueueResult::Deduplicated { .. }) => {
                        redispatch_deduplicated += 1;
                        events.push(OrchestrationEvent::new(
                            state.tenant_id.clone(),
                            state.workspace_id.clone(),
                            OrchestrationEventData::DispatchEnqueued {
                                dispatch_id: original_dispatch_id.clone(),
                                run_id: Some(run_id),
                                task_key: Some(task_key),
                                attempt: Some(attempt),
                                cloud_task_id: cloud_id,
                            },
                        ));
                    }
                    Ok(EnqueueResult::QueueFull) => {
                        redispatch_failed += 1;
                        errors.push(RunError {
                            kind: "redispatch_queue_full".to_string(),
                            id: original_dispatch_id,
                            message: "queue full".to_string(),
                        });
                    }
                    Err(err) => {
                        redispatch_failed += 1;
                        errors.push(RunError {
                            kind: "redispatch_enqueue_failed".to_string(),
                            id: original_dispatch_id,
                            message: err.to_string(),
                        });
                    }
                }
            }
            Repair::FailStaleRunningTask {
                run_id,
                task_key,
                attempt,
                attempt_id,
                reason,
            } => {
                events.push(OrchestrationEvent::new(
                    state.tenant_id.clone(),
                    state.workspace_id.clone(),
                    OrchestrationEventData::TaskFinished {
                        run_id,
                        task_key,
                        attempt,
                        attempt_id,
                        worker_id: "anti-entropy".to_string(),
                        outcome: TaskOutcome::Failed,
                        materialization_id: None,
                        error_message: Some(reason),
                        output: None,
                        error: None,
                        metrics: None,
                        cancelled_during_phase: None,
                        partial_progress_json: None,
                        asset_key: None,
                        partition_key: None,
                        code_version: None,
                    },
                ));

                repairs_created += 1;
            }
            Repair::SkippedDueToLag { .. } => {
                skipped_due_to_lag += 1;
            }
        }
    }

    if !events.is_empty() {
        append_events_and_compact(&state.ledger, state.orch_compactor_url.as_deref(), events)
            .await?;
    }

    let summary = RunSummary {
        repairs_created,
        redispatch_attempted,
        redispatch_enqueued,
        redispatch_deduplicated,
        redispatch_failed,
        skipped_due_to_lag,
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

fn redispatch_cloud_task_id(
    original_dispatch_id: &str,
    repair_epoch: &str,
    repair_attempt_id: &str,
) -> String {
    cloud_task_id(
        "d",
        &format!("{original_dispatch_id}:repair:{repair_epoch}:{repair_attempt_id}"),
    )
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
    let dispatch_target_audience_env = optional_env("ARCO_FLOW_DISPATCH_TARGET_AUDIENCE");
    let callback_base_url = required_env("ARCO_FLOW_CALLBACK_BASE_URL")?;
    let project_id = required_env("ARCO_GCP_PROJECT_ID")?;
    let location = required_env("ARCO_GCP_LOCATION")?;
    let queue_name =
        optional_env("ARCO_FLOW_QUEUE").unwrap_or_else(|| "arco-flow-dispatch".to_string());
    let orch_compactor_url = optional_env("ARCO_FLOW_COMPACTOR_URL");
    let service_account_email = optional_env("ARCO_FLOW_SERVICE_ACCOUNT_EMAIL");
    let task_timeout_secs = task_timeout_seconds_from_env()?;
    let task_token_config = task_token_config_from_env(task_timeout_secs)?;
    let worker_dispatch_headers =
        worker_dispatch_headers(&required_env("ARCO_FLOW_WORKER_DISPATCH_SECRET")?)?;
    let port = resolve_port()?;
    let dispatch_target_audience = resolve_target_audience(
        dispatch_target_audience_env.as_deref(),
        &dispatch_target_url,
    );

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
        storage: storage.clone(),
        compactor: MicroCompactor::new(storage.clone()),
        ledger: LedgerWriter::new(storage),
        orch_compactor_url,
        cloud_tasks: Arc::new(cloud_tasks),
        worker_dispatch_headers,
        dispatch_target_url,
        dispatch_target_audience,
        callback_base_url,
        task_token_config,
        clock: SweeperClock::system(),
    };

    let app = router(state);

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
    use arco_flow::orchestration::controllers::LedgerFreshness;

    use arco_core::{FlowPaths, MemoryBackend};
    use arco_flow::orchestration::callbacks::{
        CallbackContext, CallbackResult, TaskCompletedRequest, TaskState as CallbackTaskState,
        TaskStateLookup, TaskTokenValidator, WorkerOutcome, handle_task_completed,
    };
    use arco_flow::orchestration::compactor::fold::{DispatchStatus, FoldState, TaskState};
    use arco_flow::orchestration::compactor::manifest::Watermarks;
    use arco_flow::orchestration::controllers::{ReadyDispatchAction, ReadyDispatchController};
    use arco_flow::orchestration::events::{TaskDef, TriggerInfo};
    use axum::body::Body;
    use axum::http::Request;
    use std::future::Future;
    use tower::ServiceExt;

    const TENANT: &str = "tenant-wiring";
    const WORKSPACE: &str = "workspace-wiring";
    const RUN_ID: &str = "run_wiring";
    const TASK_KEY: &str = "extract";

    /// Resolves callback task state from a folded projection, mirroring what
    /// the API's Parquet-backed lookup does for the real callback route.
    struct FoldStateLookup {
        state: FoldState,
    }

    impl TaskStateLookup for FoldStateLookup {
        fn get_task_state(
            &self,
            _task_id: &str,
        ) -> impl Future<Output = std::result::Result<Option<CallbackTaskState>, String>> + Send
        {
            let row = self
                .state
                .tasks
                .get(&(RUN_ID.to_string(), TASK_KEY.to_string()))
                .cloned();
            let cancel_requested = self
                .state
                .runs
                .get(RUN_ID)
                .is_some_and(|run| run.cancel_requested);
            async move {
                Ok(row.map(|row| CallbackTaskState {
                    state: match row.state {
                        TaskState::Planned => "PLANNED",
                        TaskState::Blocked => "BLOCKED",
                        TaskState::Ready => "READY",
                        TaskState::Dispatched => "DISPATCHED",
                        TaskState::Running => "RUNNING",
                        TaskState::Succeeded => "SUCCEEDED",
                        TaskState::Failed => "FAILED",
                        TaskState::Skipped => "SKIPPED",
                        TaskState::Cancelled => "CANCELLED",
                        TaskState::RetryWait => "RETRY_WAIT",
                    }
                    .to_string(),
                    attempt: row.attempt,
                    attempt_id: row.attempt_id.clone().unwrap_or_default(),
                    run_id: row.run_id.clone(),
                    task_key: row.task_key.clone(),
                    asset_key: row.asset_key.clone(),
                    partition_key: row.partition_key,
                    code_version: None,
                    cancel_requested,
                }))
            }
        }
    }

    struct AllowAllTokens;

    impl TaskTokenValidator for AllowAllTokens {
        async fn validate_task_token(
            &self,
            _task_id: &str,
            _run_id: &str,
            _attempt: u32,
            _attempt_id: &str,
            _token: &str,
        ) -> std::result::Result<(), String> {
            Ok(())
        }
    }

    struct EnqueueAllTasks;

    #[async_trait::async_trait]
    impl CloudTaskEnqueuer for EnqueueAllTasks {
        async fn enqueue_http(
            &self,
            task_id: &str,
            _target_url: &str,
            _body: &[u8],
            _options: EnqueueOptions,
            _audience: Option<&str>,
            _extra_headers: Option<HashMap<String, String>>,
        ) -> Result<EnqueueResult> {
            Ok(EnqueueResult::Enqueued {
                message_id: task_id.to_string(),
            })
        }
    }

    fn test_state(storage: ScopedStorage, clock: SweeperClock) -> AppState {
        AppState {
            tenant_id: TENANT.to_string(),
            workspace_id: WORKSPACE.to_string(),
            storage: storage.clone(),
            compactor: MicroCompactor::new(storage.clone()),
            ledger: LedgerWriter::new(storage),
            orch_compactor_url: None,
            cloud_tasks: Arc::new(EnqueueAllTasks),
            worker_dispatch_headers: HashMap::new(),
            dispatch_target_url: "https://worker.invalid/dispatch".to_string(),
            dispatch_target_audience: "https://worker.invalid".to_string(),
            callback_base_url: "https://api.invalid".to_string(),
            task_token_config: task_token_config_from_parts(
                "secret".to_string(),
                Some("issuer".to_string()),
                Some("audience".to_string()),
                3_600,
                1_800,
            )
            .expect("task token config"),
            clock,
        }
    }

    fn memory_storage() -> ScopedStorage {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage")
    }

    /// Appends events through the real ledger and folds them with the real
    /// compactor, exactly as every deployed writer does.
    async fn append_and_compact(state: &AppState, events: Vec<OrchestrationEvent>) {
        append_events_and_compact(&state.ledger, None, events)
            .await
            .expect("append and compact");
    }

    fn run_triggered() -> OrchestrationEvent {
        OrchestrationEvent::new(
            TENANT,
            WORKSPACE,
            OrchestrationEventData::RunTriggered {
                run_id: RUN_ID.to_string(),
                plan_id: "plan_wiring".to_string(),
                trigger: TriggerInfo::Manual {
                    user_id: "tester".to_string(),
                },
                root_assets: vec![TASK_KEY.to_string()],
                run_key: None,
                labels: HashMap::new(),
                code_version: None,
            },
        )
    }

    fn plan_created(heartbeat_timeout_sec: u32) -> OrchestrationEvent {
        OrchestrationEvent::new(
            TENANT,
            WORKSPACE,
            OrchestrationEventData::PlanCreated {
                run_id: RUN_ID.to_string(),
                plan_id: "plan_wiring".to_string(),
                tasks: vec![TaskDef {
                    key: TASK_KEY.to_string(),
                    depends_on: Vec::new(),
                    asset_key: Some("analytics.extract".to_string()),
                    partition_key: None,
                    max_attempts: 3,
                    heartbeat_timeout_sec,
                    requires_visible_output: false,
                }],
            },
        )
    }

    /// Drives the real ready-dispatch controller and folds its decision, which
    /// is how attempt 1 reaches DISPATCHED in the deployed loop.
    async fn dispatch_first_attempt(state: &AppState) -> (u32, String) {
        let (manifest, fold_state) = state.compactor.load_state().await.expect("load state");
        let actions = ReadyDispatchController::with_defaults().reconcile(&manifest, &fold_state);
        let ReadyDispatchAction::EmitDispatchRequested {
            attempt,
            attempt_id,
            worker_queue,
            dispatch_id,
            ..
        } = actions
            .into_iter()
            .find(|action| matches!(action, ReadyDispatchAction::EmitDispatchRequested { .. }))
            .expect("the ready-dispatch controller must emit attempt 1")
        else {
            unreachable!("filtered to EmitDispatchRequested");
        };

        append_and_compact(
            state,
            vec![OrchestrationEvent::new(
                TENANT,
                WORKSPACE,
                OrchestrationEventData::DispatchRequested {
                    run_id: RUN_ID.to_string(),
                    task_key: TASK_KEY.to_string(),
                    attempt,
                    attempt_id: attempt_id.clone(),
                    worker_queue,
                    dispatch_id,
                },
            )],
        )
        .await;
        (attempt, attempt_id)
    }

    async fn post_run(state: AppState) -> StatusCode {
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/run")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("router response");
        response.status()
    }

    /// #337, at the wiring level: fold, real callback route, real ledger, real
    /// compactor and the real sweeper `/run` route must converge a first
    /// failure onto a dispatched second attempt. Without the fold-scheduled
    /// deadline the sweeper emits no retry dispatch at all, so this test fails
    /// on the unfixed code rather than on a hand-built event.
    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the wiring regression keeps the full failure-to-retry sequence visible"
    )]
    async fn sweeper_route_converges_a_failed_first_attempt_onto_a_dispatched_retry() {
        let storage = memory_storage();
        let state = test_state(storage.clone(), SweeperClock::system());

        append_and_compact(&state, vec![run_triggered()]).await;
        append_and_compact(&state, vec![plan_created(300)]).await;
        let (attempt, attempt_id) = dispatch_first_attempt(&state).await;
        assert_eq!(attempt, 1);

        append_and_compact(
            &state,
            vec![OrchestrationEvent::new(
                TENANT,
                WORKSPACE,
                OrchestrationEventData::TaskStarted {
                    run_id: RUN_ID.to_string(),
                    task_key: TASK_KEY.to_string(),
                    attempt,
                    attempt_id: attempt_id.clone(),
                    worker_id: "worker-1".to_string(),
                },
            )],
        )
        .await;

        // The actual worker callback route records the failure.
        let (_, fold_state) = state.compactor.load_state().await.expect("load state");
        let lookup = FoldStateLookup { state: fold_state };
        let ctx = CallbackContext::new(
            Arc::new(state.ledger.clone()),
            Arc::new(AllowAllTokens),
            TENANT,
            WORKSPACE,
        );
        let callback_id = callback_task_id(RUN_ID, TASK_KEY);
        let result = handle_task_completed(
            &ctx,
            &callback_id,
            "token",
            TaskCompletedRequest {
                attempt,
                attempt_id: attempt_id.clone(),
                worker_id: "worker-1".to_string(),
                traceparent: None,
                outcome: WorkerOutcome::Failed,
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
        assert!(
            matches!(result, CallbackResult::Ok(_)),
            "the failure callback must be accepted: {result:?}"
        );

        // Compact the callback's durable event, as the compactor would.
        compact_all_ledger_events(&state).await;

        let (_, fold_state) = state.compactor.load_state().await.expect("load state");
        let task = fold_state
            .tasks
            .get(&(RUN_ID.to_string(), TASK_KEY.to_string()))
            .expect("task row");
        assert_eq!(task.state, TaskState::RetryWait);
        let deadline = task
            .retry_not_before
            .expect("the fold must durably schedule the first retry deadline (#337)");

        // Advance the sweeper's clock past the durable deadline and invoke the
        // real route.
        let state_after_deadline = AppState {
            clock: SweeperClock {
                fixed: Some(deadline + chrono::Duration::seconds(1)),
            },
            ..state.clone()
        };
        assert_eq!(post_run(state_after_deadline).await, StatusCode::OK);

        let (_, fold_state) = state.compactor.load_state().await.expect("load state");
        let task = fold_state
            .tasks
            .get(&(RUN_ID.to_string(), TASK_KEY.to_string()))
            .expect("task row");
        assert_eq!(
            task.state,
            TaskState::Dispatched,
            "the sweeper route must dispatch the retry attempt"
        );
        assert_eq!(task.attempt, 2);
        assert_eq!(
            task.retry_not_before, None,
            "dispatching the retry clears the durable deadline"
        );

        let retry_dispatch_id = DispatchOutboxRow::dispatch_id(RUN_ID, TASK_KEY, 2);
        let outbox_row = fold_state
            .dispatch_outbox
            .get(&retry_dispatch_id)
            .expect("the retry must have a pending outbox record");
        assert_eq!(outbox_row.attempt, 2);
        assert_eq!(outbox_row.status, DispatchStatus::Pending);

        // The retry dispatch is a real ledger fact, not a test fixture.
        let ledger_events = read_ledger_events(&storage).await;
        assert!(
            ledger_events.iter().any(|event| matches!(
                &event.data,
                OrchestrationEventData::DispatchRequested { attempt: 2, .. }
            )),
            "the ledger must contain the sweeper's retry DispatchRequested"
        );
    }

    /// #338 at the wiring level, under the H4 remedy: `/run` derives ledger
    /// freshness from storage. A fully folded ledger with a current watermark
    /// reaps the zombie; an unprocessed newer event does not; and neither does
    /// a durable-but-unfolded straggler whose id sits *below* the watermark,
    /// which the freshness scan cannot see.
    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the wiring regression compares all three freshness cases in one test"
    )]
    async fn sweeper_route_reaps_zombie_only_on_evidence_it_can_actually_prove() {
        // (a) Fully folded ledger, stale RUNNING task: the reap happens.
        let storage = memory_storage();
        let state = test_state(storage.clone(), SweeperClock::system());
        seed_stale_running_task(&state).await;
        // Compaction is current by wall clock: the heartbeat was folded now,
        // and it reports liveness from outside the staleness window.
        assert_eq!(post_run(state.clone()).await, StatusCode::OK);

        let (_, fold_state) = state.compactor.load_state().await.expect("load state");
        let task = fold_state
            .tasks
            .get(&(RUN_ID.to_string(), TASK_KEY.to_string()))
            .expect("task row");
        assert_ne!(
            task.state,
            TaskState::Running,
            "a stale RUNNING task on a fully folded ledger must be reaped"
        );
        let ledger_events = read_ledger_events(&storage).await;
        assert!(
            ledger_events.iter().any(|event| matches!(
                &event.data,
                OrchestrationEventData::TaskFinished {
                    outcome: TaskOutcome::Failed,
                    worker_id,
                    ..
                } if worker_id == "anti-entropy"
            )),
            "the reap must be a real appended TaskFinished(Failed) event"
        );

        // (b) One unprocessed newer event: freshness is Stale, no reap.
        let storage = memory_storage();
        let state = test_state(storage.clone(), SweeperClock::system());
        seed_stale_running_task(&state).await;
        // Append a newer event without compacting it.
        state
            .ledger
            .append(OrchestrationEvent::new(
                TENANT,
                WORKSPACE,
                OrchestrationEventData::RunCancelRequested {
                    run_id: "run_other".to_string(),
                    requested_by: "tester".to_string(),
                    reason: None,
                },
            ))
            .await
            .expect("append unprocessed event");

        assert_eq!(post_run(state.clone()).await, StatusCode::OK);

        let (_, fold_state) = state.compactor.load_state().await.expect("load state");
        let task = fold_state
            .tasks
            .get(&(RUN_ID.to_string(), TASK_KEY.to_string()))
            .expect("task row");
        assert_eq!(
            task.state,
            TaskState::Running,
            "an unprocessed newer ledger event must block the reap"
        );

        // (c) H4: a durable-but-unfolded straggler whose id is *below* the
        // watermark. Nothing in the scan can see it, so freshness still reports
        // Current while the projection is missing that event. The reap must not
        // proceed on evidence that cannot exclude it.
        let storage = memory_storage();
        let state = test_state(storage.clone(), SweeperClock::system());
        let stale_heartbeat = seed_stale_running_task(&state).await;

        let (manifest, fold_state) = state.compactor.load_state().await.expect("load state");
        let watermark = manifest
            .watermarks
            .events_processed_through
            .clone()
            .expect("a watermark after folding");
        let watermark_ulid = Ulid::from_string(&watermark).expect("watermark is a ULID");

        // Mint the straggler one millisecond *below* the watermark and append
        // it without folding: writer A appended it, writer B folded past it.
        let mut straggler = OrchestrationEvent::new(
            TENANT,
            WORKSPACE,
            OrchestrationEventData::RunCancelRequested {
                run_id: RUN_ID.to_string(),
                requested_by: "tester".to_string(),
                reason: None,
            },
        );
        straggler.event_id = Ulid::from_parts(watermark_ulid.timestamp_ms() - 1, 0).to_string();
        assert!(
            straggler.event_id.as_str() < watermark.as_str(),
            "the straggler must sort below the fold watermark"
        );
        state
            .ledger
            .append(straggler)
            .await
            .expect("append straggler");

        let freshness =
            orchestration_ledger_freshness(&storage, &manifest.watermarks, Utc::now()).await;
        assert_eq!(
            freshness,
            LedgerFreshness::Current,
            "a maximum-id scan cannot see a straggler at or below the watermark"
        );

        let tasks: Vec<_> = fold_state.tasks.values().cloned().collect();

        // With a stale wall clock, freshness is the only thing that could
        // authorise the reap — and it must not.
        let stale_watermarks = Watermarks {
            last_processed_at: stale_heartbeat - chrono::Duration::hours(2),
            ..manifest.watermarks.clone()
        };
        let repairs = AntiEntropySweeper::with_defaults().scan_with_ledger_freshness(
            &stale_watermarks,
            freshness,
            &tasks,
            &[],
            Utc::now(),
        );
        assert!(
            !repairs.iter().any(Repair::is_destructive),
            "ledger freshness alone must not authorise the destructive reap: {repairs:?}"
        );
        assert!(
            repairs
                .iter()
                .any(|repair| matches!(repair, Repair::SkippedDueToLag { .. })),
            "the suppressed reap must be reported: {repairs:?}"
        );
    }

    /// Seeds a run whose single task is RUNNING with a stale heartbeat, folded
    /// through the real ledger and compactor. Returns the heartbeat time.
    async fn seed_stale_running_task(state: &AppState) -> chrono::DateTime<Utc> {
        append_and_compact(state, vec![run_triggered()]).await;
        append_and_compact(state, vec![plan_created(300)]).await;
        let (attempt, attempt_id) = dispatch_first_attempt(state).await;
        append_and_compact(
            state,
            vec![OrchestrationEvent::new(
                TENANT,
                WORKSPACE,
                OrchestrationEventData::TaskStarted {
                    run_id: RUN_ID.to_string(),
                    task_key: TASK_KEY.to_string(),
                    attempt,
                    attempt_id: attempt_id.clone(),
                    worker_id: "worker-1".to_string(),
                },
            )],
        )
        .await;

        // The heartbeat is folded *now* (so the compaction watermark is fresh)
        // but reports liveness from well outside the staleness window, which is
        // the shape of a worker that died mid-attempt in a busy workspace.
        let heartbeat_at = Utc::now() - chrono::Duration::seconds(400);
        let heartbeat = OrchestrationEvent::new(
            TENANT,
            WORKSPACE,
            OrchestrationEventData::TaskHeartbeat {
                run_id: RUN_ID.to_string(),
                task_key: TASK_KEY.to_string(),
                attempt,
                attempt_id,
                worker_id: "worker-1".to_string(),
                heartbeat_at: Some(heartbeat_at),
                progress_pct: None,
                message: None,
            },
        );
        append_and_compact(state, vec![heartbeat]).await;
        heartbeat_at
    }

    /// Compacts every ledger event, mirroring a compactor catching up.
    async fn compact_all_ledger_events(state: &AppState) {
        let paths: Vec<String> = state
            .storage
            .list(FlowPaths::ORCHESTRATION_LEDGER_PREFIX)
            .await
            .expect("list ledger")
            .into_iter()
            .map(|path| path.as_str().to_string())
            .collect();
        state
            .compactor
            .compact_events(paths)
            .await
            .expect("compact ledger");
    }

    /// Reads every appended ledger event.
    async fn read_ledger_events(storage: &ScopedStorage) -> Vec<OrchestrationEvent> {
        let mut paths: Vec<String> = storage
            .list(FlowPaths::ORCHESTRATION_LEDGER_PREFIX)
            .await
            .expect("list ledger")
            .into_iter()
            .map(|path| path.as_str().to_string())
            .collect();
        paths.sort();
        let mut events = Vec::new();
        for path in paths {
            let bytes = storage.get_raw(&path).await.expect("read event");
            events.push(serde_json::from_slice(&bytes).expect("parse event"));
        }
        events
    }

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

    #[test]
    fn redispatch_cloud_task_id_is_repair_scoped() {
        let original_dispatch_id = "dispatch:run1:extract:1";
        let original_cloud_id = cloud_task_id("d", original_dispatch_id);

        let repair_cloud_id =
            redispatch_cloud_task_id(original_dispatch_id, "outbox-v1", "repair-evt-1");
        let retry_repair_cloud_id =
            redispatch_cloud_task_id(original_dispatch_id, "outbox-v1", "repair-evt-2");
        let later_repair_cloud_id =
            redispatch_cloud_task_id(original_dispatch_id, "outbox-v2", "repair-evt-3");

        assert_ne!(repair_cloud_id, original_cloud_id);
        assert_ne!(retry_repair_cloud_id, repair_cloud_id);
        assert_ne!(later_repair_cloud_id, original_cloud_id);
        assert_ne!(later_repair_cloud_id, repair_cloud_id);
    }
}
