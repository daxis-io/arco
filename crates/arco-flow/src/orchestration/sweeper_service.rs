//! Operator-hosted Flow anti-entropy sweeper routes.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Serialize;
#[cfg(test)]
use ulid::Ulid;

use crate::dispatch::{EnqueueResult, HttpTaskEnqueuer, enqueue_worker_dispatch};
use crate::error::Error;
use crate::orchestration::LedgerWriter;
use crate::orchestration::compactor::MicroCompactor;
use crate::orchestration::compactor::fold::DispatchOutboxRow;
use crate::orchestration::controllers::{AntiEntropySweeper, Repair};
use crate::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TaskOutcome};
use crate::orchestration::flow_service::{
    append_events_and_compact, orchestration_ledger_freshness,
};
use crate::orchestration::ids::{cloud_task_id, deterministic_attempt_id};
use crate::orchestration::worker_contract::{DispatchEnvelopeSpec, dispatch_envelope_for_attempt};
use arco_core::{ScopedStorage, TaskTokenConfig, mint_task_token_for_attempt};
use arco_worker_contract::callback_task_id;

/// Operator-selected Flow sweeper endpoints and authentication settings.
#[derive(Clone)]
pub struct SweeperServiceConfig {
    /// Optional remote Flow compactor URL.
    pub orch_compactor_url: Option<String>,
    /// Headers delivered to the worker.
    pub worker_dispatch_headers: HashMap<String, String>,
    /// Worker dispatch endpoint.
    pub dispatch_target_url: String,
    /// Authentication audience for worker delivery.
    pub dispatch_target_audience: String,
    /// API callback base URL placed in worker envelopes.
    pub callback_base_url: String,
    /// Configuration for scoped task tokens.
    pub task_token_config: TaskTokenConfig,
    /// Maximum worker task duration used to validate token lifetime.
    pub task_timeout_secs: u64,
}

/// A sweeper bound to one workspace authority root and one queue.
#[derive(Clone)]
pub struct SweeperServiceState {
    tenant_id: String,
    workspace_id: String,
    storage: ScopedStorage,
    compactor: MicroCompactor,
    ledger: LedgerWriter,
    orch_compactor_url: Option<String>,
    task_queue: Arc<dyn HttpTaskEnqueuer>,
    worker_dispatch_headers: HashMap<String, String>,
    dispatch_target_url: String,
    dispatch_target_audience: String,
    callback_base_url: String,
    task_token_config: TaskTokenConfig,
    clock: SweeperClock,
}

impl SweeperServiceState {
    /// Binds the sweeper and queue to one workspace root before I/O.
    ///
    /// # Errors
    ///
    /// Rejects another root kind or an invalid task-token lifetime.
    pub fn new(
        storage: ScopedStorage,
        task_queue: Arc<dyn HttpTaskEnqueuer>,
        config: SweeperServiceConfig,
    ) -> Result<Self, Error> {
        let workspace_id = storage.scope().workspace_id().ok_or_else(|| {
            Error::configuration("Flow sweeper requires a workspace authority root")
        })?;
        config
            .task_token_config
            .validate_for_dispatch(config.task_timeout_secs, true)
            .map_err(|error| Error::configuration(error.to_string()))?;
        Ok(Self {
            tenant_id: storage.tenant_id().to_string(),
            workspace_id: workspace_id.to_string(),
            compactor: MicroCompactor::new(storage.clone()),
            ledger: LedgerWriter::new(storage.clone()),
            storage,
            orch_compactor_url: config.orch_compactor_url,
            task_queue,
            worker_dispatch_headers: config.worker_dispatch_headers,
            dispatch_target_url: config.dispatch_target_url,
            dispatch_target_audience: config.dispatch_target_audience,
            callback_base_url: config.callback_base_url,
            task_token_config: config.task_token_config,
            clock: SweeperClock::system(),
        })
    }
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

/// Builds the sweeper HTTP routes with an operator-supplied queue.
///
/// The caller owns listener placement, authentication, scheduling, and queue delivery.
pub fn router(state: SweeperServiceState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/run", post(run_handler))
        .with_state(state)
}

#[allow(clippy::too_many_lines)]
async fn run_handler(
    State(state): State<SweeperServiceState>,
) -> Result<Json<RunSummary>, ApiError> {
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
                let mut envelope = dispatch_envelope_for_attempt(
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
                let run = fold_state
                    .runs
                    .get(&run_id)
                    .ok_or_else(|| Error::dispatch("dispatch run is missing"))?;
                crate::orchestration::accepted_plan::populate_accepted_payload(
                    &state.ledger.storage(),
                    run,
                    &mut envelope,
                )
                .await?;

                let repair_epoch = outbox_by_id
                    .get(&original_dispatch_id)
                    .map_or("missing_dispatch_outbox", |row| row.row_version.as_str());
                let cloud_id = redispatch_cloud_task_id(&original_dispatch_id, repair_epoch);
                let result = enqueue_worker_dispatch(
                    state.task_queue.as_ref(),
                    &cloud_id,
                    &state.dispatch_target_url,
                    &state.dispatch_target_audience,
                    &state.worker_dispatch_headers,
                    &envelope,
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

fn redispatch_cloud_task_id(original_dispatch_id: &str, repair_epoch: &str) -> String {
    cloud_task_id(
        "d",
        &format!("{original_dispatch_id}:repair:{repair_epoch}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::EnqueueOptions;
    use crate::error::Result as FlowResult;
    use crate::orchestration::controllers::LedgerFreshness;

    use crate::orchestration::callbacks::{
        CallbackContext, CallbackResult, TaskCompletedRequest, TaskState as CallbackTaskState,
        TaskStateLookup, TaskTokenValidator, WorkerOutcome, handle_task_completed,
    };
    use crate::orchestration::compactor::fold::{DispatchStatus, FoldState, TaskState};
    use crate::orchestration::compactor::manifest::Watermarks;
    use crate::orchestration::controllers::{ReadyDispatchAction, ReadyDispatchController};
    use crate::orchestration::events::{TaskDef, TriggerInfo};
    use arco_core::{FlowPaths, MemoryBackend, StorageBackend};
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
        ) -> impl Future<Output = Result<Option<CallbackTaskState>, String>> + Send {
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
                    requires_visible_output: row.requires_visible_output,
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
        ) -> Result<(), String> {
            Ok(())
        }
    }

    struct EnqueueAllTasks;

    #[async_trait::async_trait]
    impl HttpTaskEnqueuer for EnqueueAllTasks {
        async fn enqueue_http(
            &self,
            task_id: &str,
            _target_url: &str,
            _body: &[u8],
            _options: EnqueueOptions,
            _audience: Option<&str>,
            _extra_headers: Option<HashMap<String, String>>,
        ) -> FlowResult<EnqueueResult> {
            Ok(EnqueueResult::Enqueued {
                message_id: task_id.to_string(),
            })
        }
    }

    fn test_state(storage: ScopedStorage, clock: SweeperClock) -> SweeperServiceState {
        SweeperServiceState {
            tenant_id: TENANT.to_string(),
            workspace_id: WORKSPACE.to_string(),
            storage: storage.clone(),
            compactor: MicroCompactor::new(storage.clone()),
            ledger: LedgerWriter::new(storage),
            orch_compactor_url: None,
            task_queue: Arc::new(EnqueueAllTasks),
            worker_dispatch_headers: HashMap::new(),
            dispatch_target_url: "https://worker.invalid/dispatch".to_string(),
            dispatch_target_audience: "https://worker.invalid".to_string(),
            callback_base_url: "https://api.invalid".to_string(),
            task_token_config: TaskTokenConfig {
                hs256_secret: "test-task-token-secret-32-bytes!!".to_string(),
                issuer: Some("issuer".to_string()),
                audience: Some("audience".to_string()),
                ttl_seconds: 3_600,
            },
            clock,
        }
    }

    fn memory_storage() -> ScopedStorage {
        let backend: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        ScopedStorage::new(backend, TENANT, WORKSPACE).expect("scoped storage")
    }

    /// Appends events through the real ledger and folds them with the real
    /// compactor, exactly as every deployed writer does.
    async fn append_and_compact(state: &SweeperServiceState, events: Vec<OrchestrationEvent>) {
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
    async fn dispatch_first_attempt(state: &SweeperServiceState) -> (u32, String) {
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

    async fn post_run(state: SweeperServiceState) -> StatusCode {
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
        let state_after_deadline = SweeperServiceState {
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
    async fn seed_stale_running_task(state: &SweeperServiceState) -> chrono::DateTime<Utc> {
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
    async fn compact_all_ledger_events(state: &SweeperServiceState) {
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
    fn redispatch_cloud_task_id_is_repair_scoped() {
        let original_dispatch_id = "dispatch:run1:extract:1";
        let original_cloud_id = cloud_task_id("d", original_dispatch_id);

        let repair_cloud_id = redispatch_cloud_task_id(original_dispatch_id, "outbox-v1");
        let retry_repair_cloud_id = redispatch_cloud_task_id(original_dispatch_id, "outbox-v1");
        let later_repair_cloud_id = redispatch_cloud_task_id(original_dispatch_id, "outbox-v2");

        assert_ne!(repair_cloud_id, original_cloud_id);
        assert_eq!(retry_repair_cloud_id, repair_cloud_id);
        assert_ne!(later_repair_cloud_id, original_cloud_id);
        assert_ne!(later_repair_cloud_id, repair_cloud_id);
    }
}
