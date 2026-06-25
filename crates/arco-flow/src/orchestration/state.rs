//! Flow-owned orchestration read models.

use std::collections::HashMap;

use arco_core::lock::{DEFAULT_LOCK_TTL, DistributedLock};
use arco_core::{ScopedStorage, VisibilityStatus};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::compaction_client::compact_orchestration_events_fenced;
use crate::orchestration::compactor::{
    DepResolution, FoldState, MicroCompactor, OrchestrationManifest, RunRow,
    RunState as FoldRunState, TaskRow, TaskState as FoldTaskState,
};
use crate::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, OutputVisibilityState,
};
use crate::orchestration::ledger::LedgerWriter;
use crate::orchestration_compaction_lock_path;

const DEFAULT_RUN_LIST_LIMIT: usize = 20;
const DEFAULT_MAX_RUN_LIST_LIMIT: usize = 100;
const LABEL_PARENT_RUN_ID: &str = "arco.parent_run_id";
const LABEL_RERUN_KIND: &str = "arco.rerun.kind";

/// Public run state exposed by flow-owned read models.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunStateView {
    /// Run is pending and has not started.
    Pending,
    /// Run is actively executing.
    Running,
    /// Run cancellation has been requested and is still propagating.
    Cancelling,
    /// Run completed successfully.
    Succeeded,
    /// Run failed.
    Failed,
    /// Run was cancelled.
    Cancelled,
    /// Run timed out.
    TimedOut,
}

/// Public task state exposed by flow-owned read models.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskStateView {
    /// Task is pending dependency satisfaction or retry delay.
    Pending,
    /// Task is ready or dispatched.
    Queued,
    /// Task is actively running.
    Running,
    /// Task completed successfully.
    Succeeded,
    /// Task failed.
    Failed,
    /// Task was skipped.
    Skipped,
    /// Task was cancelled.
    Cancelled,
}

/// Public output visibility state for output-producing tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OutputVisibilityStateView {
    /// Output exists but is not yet consumable.
    Pending,
    /// Output is visible and consumable.
    Visible,
    /// Output permanently failed to become visible.
    Failed,
}

/// Task counts grouped by public state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskCounts {
    /// Total number of tasks in the run.
    pub total: u32,
    /// Number of public-pending tasks.
    pub pending: u32,
    /// Number of public-queued tasks.
    pub queued: u32,
    /// Number of running tasks.
    pub running: u32,
    /// Number of succeeded tasks.
    pub succeeded: u32,
    /// Number of failed tasks.
    pub failed: u32,
    /// Number of skipped tasks.
    pub skipped: u32,
    /// Number of cancelled tasks.
    pub cancelled: u32,
}

/// Retry attribution for tasks that consumed retries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskRetryAttribution {
    /// Number of retries consumed before the current attempt.
    pub retries: u32,
    /// Stable reason code for the retry attribution.
    pub reason: String,
}

/// Skip attribution for tasks skipped due to upstream outcomes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSkipAttribution {
    /// Upstream task key that caused this skip.
    pub upstream_task_key: String,
    /// Upstream dependency resolution that propagated the skip.
    pub upstream_resolution: String,
}

/// Summary of one task within a run detail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskSummary {
    /// Task key within the run.
    pub task_key: String,
    /// Asset key associated with the task, when this is an asset task.
    pub asset_key: Option<String>,
    /// Public task state.
    pub state: TaskStateView,
    /// Current attempt number.
    pub attempt: u32,
    /// Timestamp when the current task attempt started.
    pub started_at: Option<DateTime<Utc>>,
    /// Timestamp when the task reached a terminal state.
    pub completed_at: Option<DateTime<Utc>>,
    /// Terminal error message, when available.
    pub error_message: Option<String>,
    /// Deterministic execution lineage reference for successful materializations.
    pub execution_lineage_ref: Option<String>,
    /// Delta table recorded for this materialization.
    pub delta_table: Option<String>,
    /// Delta version recorded for this materialization.
    pub delta_version: Option<i64>,
    /// Delta partition recorded for this materialization.
    pub delta_partition: Option<String>,
    /// Retry attribution for tasks that consumed retries.
    pub retry_attribution: Option<TaskRetryAttribution>,
    /// Skip attribution for tasks skipped due to upstream outcomes.
    pub skip_attribution: Option<TaskSkipAttribution>,
    /// Output visibility state for required-output tasks.
    pub output_visibility_state: Option<OutputVisibilityStateView>,
    /// Timestamp when output became visible.
    pub published_at: Option<DateTime<Utc>>,
    /// Publish failure details for output visibility failures.
    pub publish_error: Option<String>,
}

/// Full run read model built from the compacted projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunDetail {
    /// Run identifier.
    pub run_id: String,
    /// Plan identifier.
    pub plan_id: String,
    /// Public run state.
    pub state: RunStateView,
    /// Timestamp when the run was created.
    pub created_at: DateTime<Utc>,
    /// Earliest task start timestamp in the run.
    pub started_at: Option<DateTime<Utc>>,
    /// Public completion timestamp, hidden while the public state is non-terminal.
    pub completed_at: Option<DateTime<Utc>>,
    /// Task summaries for the run.
    pub tasks: Vec<TaskSummary>,
    /// Task counts grouped by public state.
    pub task_counts: TaskCounts,
    /// Run labels.
    pub labels: HashMap<String, String>,
    /// Parent run identifier for reruns.
    pub parent_run_id: Option<String>,
    /// Rerun kind label when present and recognized.
    pub rerun_kind: Option<String>,
    /// Stable operator-facing rerun reason when present and recognized.
    pub rerun_reason: Option<String>,
}

/// Summary read model for listing runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunListItem {
    /// Run identifier.
    pub run_id: String,
    /// Public run state.
    pub state: RunStateView,
    /// Timestamp when the run was created.
    pub created_at: DateTime<Utc>,
    /// Public completion timestamp, hidden while the public state is non-terminal.
    pub completed_at: Option<DateTime<Utc>>,
    /// Total task count.
    pub task_count: u32,
    /// Number of succeeded tasks.
    pub tasks_succeeded: u32,
    /// Number of failed tasks.
    pub tasks_failed: u32,
    /// Parent run identifier for reruns.
    pub parent_run_id: Option<String>,
    /// Rerun kind label when present and recognized.
    pub rerun_kind: Option<String>,
}

/// Page of run list items.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunListPage {
    /// Runs in the current page.
    pub runs: Vec<RunListItem>,
    /// Cursor for the next page, when more items exist.
    pub next_cursor: Option<String>,
}

/// Query options for listing runs from a projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunListQuery {
    /// Maximum number of runs to return.
    pub limit: usize,
    /// Opaque cursor returned by prior pages.
    ///
    /// Legacy numeric offset cursors are accepted as input for compatibility,
    /// but newly emitted cursors are keyset cursors.
    pub cursor: Option<String>,
    /// Optional public state filter.
    pub state: Option<RunStateView>,
}

impl Default for RunListQuery {
    fn default() -> Self {
        Self {
            limit: DEFAULT_RUN_LIST_LIMIT,
            cursor: None,
            state: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RunListKeysetCursor {
    triggered_at: DateTime<Utc>,
    run_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DecodedRunListCursor {
    Keyset(RunListKeysetCursor),
    LegacyOffset(usize),
}

/// Full task read model built from the compacted projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskDetail {
    /// Run identifier.
    pub run_id: String,
    /// Task key within the run.
    pub task_key: String,
    /// Asset key associated with the task, when this is an asset task.
    pub asset_key: Option<String>,
    /// Partition key associated with the task, when present.
    pub partition_key: Option<String>,
    /// Public task state.
    pub state: TaskStateView,
    /// Current attempt number.
    pub attempt: u32,
    /// Current attempt identifier.
    pub attempt_id: Option<String>,
    /// Timestamp when the current task attempt started.
    pub started_at: Option<DateTime<Utc>>,
    /// Timestamp when the task reached a terminal state.
    pub completed_at: Option<DateTime<Utc>>,
    /// Terminal error message, when available.
    pub error_message: Option<String>,
    /// Total dependency count for this task.
    pub deps_total: u32,
    /// Number of dependencies currently satisfied.
    pub deps_satisfied_count: u32,
    /// Maximum allowed attempts.
    pub max_attempts: u32,
    /// Heartbeat timeout in seconds.
    pub heartbeat_timeout_sec: u32,
    /// Last heartbeat timestamp.
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    /// Timestamp when the task became ready.
    pub ready_at: Option<DateTime<Utc>>,
    /// Whether this task requires visible output before public run success.
    pub requires_visible_output: bool,
    /// Materialization identifier from worker output.
    pub materialization_id: Option<String>,
    /// Output visibility state for output-producing tasks.
    pub output_visibility_state: Option<OutputVisibilityStateView>,
    /// Timestamp when output became visible.
    pub published_at: Option<DateTime<Utc>>,
    /// Publish failure details for output visibility failures.
    pub publish_error: Option<String>,
    /// Delta table recorded for this materialization.
    pub delta_table: Option<String>,
    /// Delta version recorded for this materialization.
    pub delta_version: Option<i64>,
    /// Delta partition recorded for this materialization.
    pub delta_partition: Option<String>,
    /// Deterministic execution lineage reference for successful materializations.
    pub execution_lineage_ref: Option<String>,
}

/// Snapshot of orchestration read models derived from a projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrchestrationSnapshot {
    /// Visible manifest identifier used to load this snapshot.
    pub manifest_id: String,
    /// Manifest revision ULID used to load this snapshot.
    pub manifest_revision: String,
    /// Timestamp when the manifest was published.
    pub published_at: DateTime<Utc>,
    /// Run details in newest-first order.
    pub runs: Vec<RunDetail>,
}

/// Embeddable orchestration read/mutation service over scoped object storage.
#[derive(Clone)]
pub struct OrchestrationStateService {
    storage: ScopedStorage,
    options: OrchestrationStateServiceOptions,
}

/// Options for embeddable orchestration state service behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestrationStateServiceOptions {
    /// Optional remote orchestration compactor base URL.
    pub orchestration_compactor_url: Option<String>,
    /// Optional request identifier forwarded to remote compaction.
    pub request_id: Option<String>,
    /// Maximum service-level list limit accepted by API adapters.
    pub max_run_list_limit: usize,
}

impl Default for OrchestrationStateServiceOptions {
    fn default() -> Self {
        Self {
            orchestration_compactor_url: None,
            request_id: None,
            max_run_list_limit: DEFAULT_MAX_RUN_LIST_LIMIT,
        }
    }
}

impl OrchestrationStateService {
    /// Creates a state service for one tenant/workspace storage scope.
    #[must_use]
    pub fn new(storage: ScopedStorage) -> Self {
        Self {
            storage,
            options: OrchestrationStateServiceOptions::default(),
        }
    }

    /// Creates a state service with explicit options.
    #[must_use]
    pub fn with_options(storage: ScopedStorage, options: OrchestrationStateServiceOptions) -> Self {
        Self { storage, options }
    }

    /// Configures a remote orchestration compactor URL for mutations.
    #[must_use]
    pub fn with_orchestration_compactor_url(mut self, url: impl Into<String>) -> Self {
        self.options.orchestration_compactor_url = Some(url.into());
        self
    }

    /// Configures a request identifier forwarded to remote compaction.
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.options.request_id = Some(request_id.into());
        self
    }

    /// Configures the maximum accepted run-list limit.
    #[must_use]
    pub const fn with_max_run_list_limit(mut self, max_run_list_limit: usize) -> Self {
        self.options.max_run_list_limit = max_run_list_limit;
        self
    }

    /// Loads the visible orchestration projection as stable read models.
    ///
    /// # Errors
    ///
    /// Returns an error if the compacted projection cannot be loaded.
    pub async fn load_snapshot(&self) -> StateServiceResult<OrchestrationSnapshot> {
        let (manifest, state) = self.load_state().await?;
        Ok(snapshot_from_state(&manifest, &state))
    }

    /// Looks up one run from the visible projection.
    ///
    /// # Errors
    ///
    /// Returns an error if the compacted projection cannot be loaded.
    pub async fn get_run(&self, run_id: &str) -> StateServiceResult<Option<RunDetail>> {
        let (_, state) = self.load_state().await?;
        Ok(run_detail_from_state(&state, run_id))
    }

    /// Lists runs from the visible projection.
    ///
    /// # Errors
    ///
    /// Returns an error if the compacted projection cannot be loaded.
    pub async fn list_runs(&self, query: RunListQuery) -> StateServiceResult<RunListPage> {
        self.validate_run_list_query(&query)?;
        let (_, state) = self.load_state().await?;
        Ok(list_runs_from_state(&state, &query))
    }

    /// Looks up one task by run identifier and task key from the visible projection.
    ///
    /// # Errors
    ///
    /// Returns an error if the compacted projection cannot be loaded.
    pub async fn get_task(
        &self,
        run_id: &str,
        task_key: &str,
    ) -> StateServiceResult<Option<TaskDetail>> {
        let (_, state) = self.load_state().await?;
        Ok(task_detail_from_state(&state, run_id, task_key))
    }

    /// Requests cooperative cancellation for a non-terminal run.
    ///
    /// # Errors
    ///
    /// Returns not-found for missing runs, terminal-run rejection for terminal
    /// runs, or storage/compaction errors from the underlying ledger path.
    pub async fn cancel_run(
        &self,
        request: CancelRunRequest,
    ) -> StateServiceResult<CancelRunOutcome> {
        let CancelRunRequest {
            run_id,
            reason,
            requested_by,
        } = request;
        let lock_path = orchestration_compaction_lock_path();
        let lock = DistributedLock::new(self.storage.backend().clone(), lock_path);
        let guard = lock
            .acquire(DEFAULT_LOCK_TTL, 10)
            .await
            .map_err(crate::error::Error::from)?;

        let outcome = async {
            let (_, state) = self.load_state().await?;
            let run = state
                .runs
                .get(&run_id)
                .ok_or_else(|| StateServiceError::RunNotFound {
                    run_id: run_id.clone(),
                })?;
            let tasks = tasks_for_run(&state, &run_id);
            let current_state = public_run_state(run, &tasks);

            if run.cancel_requested {
                return Ok(CancelRunOutcome {
                    run_id,
                    state: current_state,
                    disposition: CancelRunDisposition::AlreadyRequested,
                });
            }

            if run.state.is_terminal() {
                return Err(StateServiceError::TerminalRun {
                    run_id,
                    state: map_run_state(run.state),
                });
            }

            let ledger = LedgerWriter::new(self.storage.clone());
            let event = OrchestrationEvent::new(
                self.storage.tenant_id(),
                self.storage.workspace_id(),
                OrchestrationEventData::RunCancelRequested {
                    run_id: run_id.clone(),
                    reason,
                    requested_by,
                },
            );
            let event_paths = vec![LedgerWriter::event_path(&event)];
            ledger.append_all(vec![event]).await?;
            self.compact_event_paths_visible(
                event_paths,
                guard.fencing_token().sequence(),
                lock_path,
            )
            .await?;

            let (_, state) = self.load_state().await?;
            let run = run_detail_from_state(&state, &run_id).ok_or_else(|| {
                StateServiceError::RunNotFound {
                    run_id: run_id.clone(),
                }
            })?;

            Ok(CancelRunOutcome {
                run_id,
                state: run.state,
                disposition: CancelRunDisposition::Requested,
            })
        }
        .await;

        if let Err(error) = guard.release().await {
            tracing::warn!(
                error = %error,
                "failed to release orchestration compaction lock after state service cancel"
            );
        }

        outcome
    }

    async fn load_state(&self) -> StateServiceResult<(OrchestrationManifest, FoldState)> {
        Ok(MicroCompactor::new(self.storage.clone())
            .load_state()
            .await?)
    }

    fn validate_run_list_query(&self, query: &RunListQuery) -> StateServiceResult<()> {
        let max = self.options.max_run_list_limit;
        if query.limit == 0 {
            return Err(StateServiceError::ZeroRunListLimit { max });
        }
        if query.limit > max {
            return Err(StateServiceError::RunListLimitTooLarge {
                limit: query.limit,
                max,
            });
        }
        if let Some(cursor) = &query.cursor {
            decode_run_list_cursor(cursor).map_err(|()| {
                StateServiceError::InvalidRunListCursor {
                    cursor: cursor.clone(),
                }
            })?;
        }
        Ok(())
    }

    async fn compact_event_paths_visible(
        &self,
        event_paths: Vec<String>,
        fencing_token: u64,
        lock_path: &str,
    ) -> StateServiceResult<()> {
        if let Some(url) = self.options.orchestration_compactor_url.as_deref() {
            let response = compact_orchestration_events_fenced(
                url,
                event_paths,
                fencing_token,
                lock_path,
                self.options.request_id.as_deref(),
            )
            .await?;
            if response.visibility_status != VisibilityStatus::Visible {
                return Err(crate::error::Error::dispatch(format!(
                    "orchestration compaction did not become visible: {}",
                    response.visibility_status.as_str()
                ))
                .into());
            }
            return Ok(());
        }

        let result = MicroCompactor::new(self.storage.clone())
            .compact_events_fenced(event_paths, fencing_token, lock_path)
            .await?;
        if result.visibility_status.as_str() != VisibilityStatus::Visible.as_str() {
            return Err(crate::error::Error::dispatch(format!(
                "orchestration compaction did not become visible: {}",
                result.visibility_status.as_str()
            ))
            .into());
        }
        Ok(())
    }
}

impl From<ScopedStorage> for OrchestrationStateService {
    fn from(storage: ScopedStorage) -> Self {
        Self::new(storage)
    }
}

/// Request to cancel a run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelRunRequest {
    /// Run identifier.
    pub run_id: String,
    /// Optional operator-facing cancellation reason.
    pub reason: Option<String>,
    /// Principal or service that requested cancellation.
    pub requested_by: String,
}

/// Outcome of a cancel request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelRunOutcome {
    /// Run identifier.
    pub run_id: String,
    /// Public run state after the request was evaluated.
    pub state: RunStateView,
    /// Whether this call appended a new cancel event or observed an existing request.
    pub disposition: CancelRunDisposition,
}

/// Disposition for a successful cancel request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CancelRunDisposition {
    /// A new `RunCancelRequested` event was appended and visibly compacted.
    Requested,
    /// Cancellation had already been requested for this non-terminal run.
    AlreadyRequested,
}

/// Errors returned by the embeddable orchestration state service.
#[derive(Debug, thiserror::Error)]
pub enum StateServiceError {
    /// The run was not found in the visible projection.
    #[error("run not found: {run_id}")]
    RunNotFound {
        /// Run identifier.
        run_id: String,
    },
    /// The run is already terminal and cannot accept a new cancellation request.
    #[error("run {run_id} is already terminal: {state:?}")]
    TerminalRun {
        /// Run identifier.
        run_id: String,
        /// Public terminal state.
        state: RunStateView,
    },
    /// Run list cursor was not a valid numeric offset.
    #[error("invalid run list cursor: {cursor}")]
    InvalidRunListCursor {
        /// Invalid cursor value.
        cursor: String,
    },
    /// Run list limit was zero.
    #[error("run list limit must be at least 1 and at most {max}")]
    ZeroRunListLimit {
        /// Maximum configured list limit.
        max: usize,
    },
    /// Run list limit exceeded the configured maximum.
    #[error("run list limit {limit} exceeds maximum {max}")]
    RunListLimitTooLarge {
        /// Requested list limit.
        limit: usize,
        /// Maximum configured list limit.
        max: usize,
    },
    /// Underlying flow storage, serialization, or compaction error.
    #[error(transparent)]
    Flow(#[from] crate::error::Error),
}

/// Result type for the embeddable orchestration state service.
pub type StateServiceResult<T> = Result<T, StateServiceError>;

/// Builds a run detail from the compacted fold state.
#[must_use]
pub fn run_detail_from_state(state: &FoldState, run_id: &str) -> Option<RunDetail> {
    let run = state.runs.get(run_id)?;
    let tasks = tasks_for_run(state, run_id);
    let skip_attribution_index = build_skip_attribution_index(state, run_id);
    Some(run_detail_from_row(run, &tasks, &skip_attribution_index))
}

/// Lists runs from the compacted fold state.
#[must_use]
pub fn list_runs_from_state(state: &FoldState, query: &RunListQuery) -> RunListPage {
    let tasks_by_run = build_tasks_by_run(state);
    let decoded_cursor = query
        .cursor
        .as_deref()
        .and_then(|cursor| decode_run_list_cursor(cursor).ok());
    let mut runs = state
        .runs
        .values()
        .filter(|run| {
            query.state.is_none_or(|state_filter| {
                let tasks = indexed_tasks_for_run(&tasks_by_run, &run.run_id);
                public_run_state(run, tasks) == state_filter
            })
        })
        .collect::<Vec<_>>();

    if let Some(DecodedRunListCursor::Keyset(cursor)) = decoded_cursor.as_ref() {
        runs.retain(|run| run_sorts_after_cursor(run, cursor));
    }

    runs.sort_by(|left, right| {
        right
            .triggered_at
            .cmp(&left.triggered_at)
            .then_with(|| left.run_id.cmp(&right.run_id))
    });

    let offset = match decoded_cursor {
        Some(DecodedRunListCursor::LegacyOffset(offset)) => offset,
        Some(DecodedRunListCursor::Keyset(_)) | None => 0,
    };
    let limit = if query.limit == 0 {
        DEFAULT_RUN_LIST_LIMIT
    } else {
        query.limit
    };
    let end = offset.saturating_add(limit).min(runs.len());
    let page = runs.get(offset..end).unwrap_or_default();

    let items = page
        .iter()
        .map(|run| {
            let tasks = indexed_tasks_for_run(&tasks_by_run, &run.run_id);
            run_list_item_from_row(run, tasks)
        })
        .collect();

    RunListPage {
        runs: items,
        next_cursor: (end < runs.len()).then(|| {
            page.last()
                .map_or_else(|| end.to_string(), |run| encode_run_list_cursor(run))
        }),
    }
}

fn decode_run_list_cursor(cursor: &str) -> Result<DecodedRunListCursor, ()> {
    if let Ok(offset) = cursor.parse::<usize>() {
        return Ok(DecodedRunListCursor::LegacyOffset(offset));
    }

    let decoded = URL_SAFE_NO_PAD.decode(cursor.as_bytes()).map_err(|_| ())?;
    let keyset = serde_json::from_slice::<RunListKeysetCursor>(&decoded).map_err(|_| ())?;
    Ok(DecodedRunListCursor::Keyset(keyset))
}

fn encode_run_list_cursor(run: &RunRow) -> String {
    let cursor = RunListKeysetCursor {
        triggered_at: run.triggered_at,
        run_id: run.run_id.clone(),
    };
    #[allow(clippy::expect_used)]
    let encoded = serde_json::to_vec(&cursor).expect("run list cursor serialization is infallible");
    URL_SAFE_NO_PAD.encode(encoded)
}

fn run_sorts_after_cursor(run: &RunRow, cursor: &RunListKeysetCursor) -> bool {
    run.triggered_at < cursor.triggered_at
        || (run.triggered_at == cursor.triggered_at && run.run_id > cursor.run_id)
}

/// Looks up a task detail by run identifier and task key.
#[must_use]
pub fn task_detail_from_state(
    state: &FoldState,
    run_id: &str,
    task_key: &str,
) -> Option<TaskDetail> {
    state
        .tasks
        .get(&(run_id.to_string(), task_key.to_string()))
        .map(task_detail_from_row)
}

fn snapshot_from_state(
    manifest: &OrchestrationManifest,
    state: &FoldState,
) -> OrchestrationSnapshot {
    let tasks_by_run = build_tasks_by_run(state);
    let skip_attribution_by_run = build_skip_attribution_indexes_by_run(state);
    let empty_skip_attribution = HashMap::new();
    let mut run_rows = state.runs.values().collect::<Vec<_>>();
    run_rows.sort_by(|left, right| {
        right
            .triggered_at
            .cmp(&left.triggered_at)
            .then_with(|| left.run_id.cmp(&right.run_id))
    });
    let runs = run_rows
        .into_iter()
        .map(|run| {
            let tasks = indexed_tasks_for_run(&tasks_by_run, &run.run_id);
            let skip_attribution_index = skip_attribution_by_run
                .get(&run.run_id)
                .unwrap_or(&empty_skip_attribution);
            run_detail_from_row(run, tasks, skip_attribution_index)
        })
        .collect();

    OrchestrationSnapshot {
        manifest_id: manifest.manifest_id.clone(),
        manifest_revision: manifest.revision_ulid.clone(),
        published_at: manifest.published_at,
        runs,
    }
}

fn run_detail_from_row(
    run: &RunRow,
    tasks: &[&TaskRow],
    skip_attribution_index: &HashMap<String, TaskSkipAttribution>,
) -> RunDetail {
    let public_state = public_run_state(run, tasks);
    let (parent_run_id, rerun_kind) = lineage_from_labels(&run.labels);
    let rerun_reason = rerun_kind
        .as_deref()
        .and_then(rerun_reason_for_kind)
        .map(ToString::to_string);

    RunDetail {
        run_id: run.run_id.clone(),
        plan_id: run.plan_id.clone(),
        state: public_state,
        created_at: run.triggered_at,
        started_at: tasks.iter().filter_map(|task| task.started_at).min(),
        completed_at: public_run_completed_at(public_state, run.completed_at),
        tasks: tasks
            .iter()
            .map(|task| task_summary_from_row(task, skip_attribution_index))
            .collect(),
        task_counts: task_counts_from_rows(run, tasks),
        labels: run.labels.clone(),
        parent_run_id,
        rerun_kind,
        rerun_reason,
    }
}

fn run_list_item_from_row(run: &RunRow, tasks: &[&TaskRow]) -> RunListItem {
    let public_state = public_run_state(run, tasks);
    let (parent_run_id, rerun_kind) = lineage_from_labels(&run.labels);

    RunListItem {
        run_id: run.run_id.clone(),
        state: public_state,
        created_at: run.triggered_at,
        completed_at: public_run_completed_at(public_state, run.completed_at),
        task_count: run.tasks_total,
        tasks_succeeded: run.tasks_succeeded,
        tasks_failed: run.tasks_failed,
        parent_run_id,
        rerun_kind,
    }
}

fn task_summary_from_row(
    task: &TaskRow,
    skip_attribution_index: &HashMap<String, TaskSkipAttribution>,
) -> TaskSummary {
    TaskSummary {
        task_key: task.task_key.clone(),
        asset_key: task.asset_key.clone(),
        state: map_task_state(task.state),
        attempt: task.attempt,
        started_at: task.started_at,
        completed_at: task.completed_at,
        error_message: task.error_message.clone(),
        execution_lineage_ref: task.execution_lineage_ref.clone(),
        delta_table: task.delta_table.clone(),
        delta_version: task.delta_version,
        delta_partition: task.delta_partition.clone(),
        retry_attribution: task_retry_attribution(task),
        skip_attribution: task_skip_attribution(task, skip_attribution_index),
        output_visibility_state: task
            .output_visibility_state
            .map(map_output_visibility_state),
        published_at: task.published_at,
        publish_error: task.publish_error.clone(),
    }
}

fn task_detail_from_row(task: &TaskRow) -> TaskDetail {
    TaskDetail {
        run_id: task.run_id.clone(),
        task_key: task.task_key.clone(),
        asset_key: task.asset_key.clone(),
        partition_key: task.partition_key.clone(),
        state: map_task_state(task.state),
        attempt: task.attempt,
        attempt_id: task.attempt_id.clone(),
        started_at: task.started_at,
        completed_at: task.completed_at,
        error_message: task.error_message.clone(),
        deps_total: task.deps_total,
        deps_satisfied_count: task.deps_satisfied_count,
        max_attempts: task.max_attempts,
        heartbeat_timeout_sec: task.heartbeat_timeout_sec,
        last_heartbeat_at: task.last_heartbeat_at,
        ready_at: task.ready_at,
        requires_visible_output: task.requires_visible_output,
        materialization_id: task.materialization_id.clone(),
        output_visibility_state: task
            .output_visibility_state
            .map(map_output_visibility_state),
        published_at: task.published_at,
        publish_error: task.publish_error.clone(),
        delta_table: task.delta_table.clone(),
        delta_version: task.delta_version,
        delta_partition: task.delta_partition.clone(),
        execution_lineage_ref: task.execution_lineage_ref.clone(),
    }
}

fn tasks_for_run<'a>(state: &'a FoldState, run_id: &str) -> Vec<&'a TaskRow> {
    let mut tasks = state
        .tasks
        .values()
        .filter(|task| task.run_id == run_id)
        .collect::<Vec<_>>();
    tasks.sort_by(|left, right| left.task_key.cmp(&right.task_key));
    tasks
}

fn build_tasks_by_run(state: &FoldState) -> HashMap<&str, Vec<&TaskRow>> {
    let mut tasks_by_run: HashMap<&str, Vec<&TaskRow>> = HashMap::new();
    for task in state.tasks.values() {
        tasks_by_run
            .entry(task.run_id.as_str())
            .or_default()
            .push(task);
    }
    for tasks in tasks_by_run.values_mut() {
        tasks.sort_by(|left, right| left.task_key.cmp(&right.task_key));
    }
    tasks_by_run
}

fn indexed_tasks_for_run<'a>(
    tasks_by_run: &'a HashMap<&str, Vec<&'a TaskRow>>,
    run_id: &str,
) -> &'a [&'a TaskRow] {
    tasks_by_run
        .get(run_id)
        .map_or(&[], |tasks| tasks.as_slice())
}

fn task_counts_from_rows(run: &RunRow, tasks: &[&TaskRow]) -> TaskCounts {
    let mut counts = TaskCounts {
        total: run.tasks_total,
        succeeded: run.tasks_succeeded,
        failed: run.tasks_failed,
        skipped: run.tasks_skipped,
        cancelled: run.tasks_cancelled,
        ..TaskCounts::default()
    };

    for task in tasks {
        match map_task_state(task.state) {
            TaskStateView::Pending => counts.pending += 1,
            TaskStateView::Queued => counts.queued += 1,
            TaskStateView::Running => counts.running += 1,
            TaskStateView::Succeeded
            | TaskStateView::Failed
            | TaskStateView::Skipped
            | TaskStateView::Cancelled => {}
        }
    }

    counts
}

fn public_run_state(run: &RunRow, tasks: &[&TaskRow]) -> RunStateView {
    if run.cancel_requested && !run.state.is_terminal() {
        return RunStateView::Cancelling;
    }
    if run.state == FoldRunState::Cancelled {
        return RunStateView::Cancelled;
    }
    if run.state == FoldRunState::Failed
        || tasks
            .iter()
            .any(|task| task_has_failed_required_output(task))
    {
        return RunStateView::Failed;
    }
    if run.state == FoldRunState::Succeeded
        && tasks
            .iter()
            .any(|task| task_has_pending_required_output(task))
    {
        return RunStateView::Running;
    }
    map_run_state(run.state)
}

fn public_run_completed_at(
    state: RunStateView,
    completed_at: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    match state {
        RunStateView::Pending | RunStateView::Running | RunStateView::Cancelling => None,
        RunStateView::Succeeded
        | RunStateView::Failed
        | RunStateView::Cancelled
        | RunStateView::TimedOut => completed_at,
    }
}

fn map_run_state(state: FoldRunState) -> RunStateView {
    match state {
        FoldRunState::Triggered => RunStateView::Pending,
        FoldRunState::Running => RunStateView::Running,
        FoldRunState::Succeeded => RunStateView::Succeeded,
        FoldRunState::Failed => RunStateView::Failed,
        FoldRunState::Cancelled => RunStateView::Cancelled,
    }
}

fn map_task_state(state: FoldTaskState) -> TaskStateView {
    match state {
        FoldTaskState::Planned | FoldTaskState::Blocked | FoldTaskState::RetryWait => {
            TaskStateView::Pending
        }
        FoldTaskState::Ready | FoldTaskState::Dispatched => TaskStateView::Queued,
        FoldTaskState::Running => TaskStateView::Running,
        FoldTaskState::Succeeded => TaskStateView::Succeeded,
        FoldTaskState::Failed => TaskStateView::Failed,
        FoldTaskState::Skipped => TaskStateView::Skipped,
        FoldTaskState::Cancelled => TaskStateView::Cancelled,
    }
}

fn map_output_visibility_state(state: OutputVisibilityState) -> OutputVisibilityStateView {
    match state {
        OutputVisibilityState::Pending => OutputVisibilityStateView::Pending,
        OutputVisibilityState::Visible => OutputVisibilityStateView::Visible,
        OutputVisibilityState::Failed => OutputVisibilityStateView::Failed,
    }
}

fn task_retry_attribution(task: &TaskRow) -> Option<TaskRetryAttribution> {
    if task.attempt <= 1 {
        return None;
    }

    Some(TaskRetryAttribution {
        retries: task.attempt.saturating_sub(1),
        reason: "prior_attempt_failed".to_string(),
    })
}

fn dep_resolution_str(resolution: DepResolution) -> &'static str {
    match resolution {
        DepResolution::Success => "SUCCESS",
        DepResolution::Failed => "FAILED",
        DepResolution::Skipped => "SKIPPED",
        DepResolution::Cancelled => "CANCELLED",
    }
}

fn build_skip_attribution_index(
    state: &FoldState,
    run_id: &str,
) -> HashMap<String, TaskSkipAttribution> {
    build_skip_attribution_indexes_by_run(state)
        .remove(run_id)
        .unwrap_or_default()
}

fn build_skip_attribution_indexes_by_run(
    state: &FoldState,
) -> HashMap<String, HashMap<String, TaskSkipAttribution>> {
    let mut first_causes: HashMap<(String, String), (i64, String, String, DepResolution)> =
        HashMap::new();

    for edge in state.dep_satisfaction.values() {
        let Some(resolution) = edge.resolution else {
            continue;
        };
        if matches!(resolution, DepResolution::Success) {
            continue;
        }

        let satisfied_at = edge
            .satisfied_at
            .map_or(0, |timestamp| timestamp.timestamp_millis());
        let upstream_task_key = edge.upstream_task_key.clone();
        let row_version = edge.row_version.clone();

        let replace = first_causes
            .get(&(edge.run_id.clone(), edge.downstream_task_key.clone()))
            .is_none_or(|current| {
                (
                    satisfied_at,
                    upstream_task_key.as_str(),
                    row_version.as_str(),
                ) < (current.0, current.1.as_str(), current.2.as_str())
            });

        if replace {
            first_causes.insert(
                (edge.run_id.clone(), edge.downstream_task_key.clone()),
                (satisfied_at, upstream_task_key, row_version, resolution),
            );
        }
    }

    let mut by_run: HashMap<String, HashMap<String, TaskSkipAttribution>> = HashMap::new();
    for ((run_id, downstream_task_key), (_, upstream_task_key, _, resolution)) in first_causes {
        by_run.entry(run_id).or_default().insert(
            downstream_task_key,
            TaskSkipAttribution {
                upstream_task_key,
                upstream_resolution: dep_resolution_str(resolution).to_string(),
            },
        );
    }
    by_run
}

fn task_skip_attribution(
    task: &TaskRow,
    skip_attribution_index: &HashMap<String, TaskSkipAttribution>,
) -> Option<TaskSkipAttribution> {
    if task.state != FoldTaskState::Skipped {
        return None;
    }

    skip_attribution_index.get(&task.task_key).cloned()
}

fn task_has_failed_required_output(task: &TaskRow) -> bool {
    task.requires_visible_output
        && task.state == FoldTaskState::Succeeded
        && task.output_visibility_state == Some(OutputVisibilityState::Failed)
}

fn task_has_pending_required_output(task: &TaskRow) -> bool {
    task.requires_visible_output
        && task.state == FoldTaskState::Succeeded
        && task.output_visibility_state == Some(OutputVisibilityState::Pending)
}

fn lineage_from_labels(labels: &HashMap<String, String>) -> (Option<String>, Option<String>) {
    let parent = labels.get(LABEL_PARENT_RUN_ID).cloned();
    let kind = labels
        .get(LABEL_RERUN_KIND)
        .and_then(|value| rerun_kind_from_label(value));
    (parent, kind)
}

fn rerun_kind_from_label(value: &str) -> Option<String> {
    match value {
        "FROM_FAILURE" | "SUBSET" => Some(value.to_string()),
        _ => None,
    }
}

fn rerun_reason_for_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "FROM_FAILURE" => Some("from_failure_unsucceeded_tasks"),
        "SUBSET" => Some("subset_selection"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arco_core::lock::{DEFAULT_LOCK_TTL, DistributedLock};
    use arco_core::{MemoryBackend, ScopedStorage};
    use chrono::{TimeZone, Utc};

    use super::{
        CancelRunDisposition, CancelRunRequest, OrchestrationStateService, RunDetail, RunListQuery,
        RunStateView, StateServiceError, TaskDetail, TaskRetryAttribution, TaskSkipAttribution,
        TaskStateView, list_runs_from_state, run_detail_from_state, task_detail_from_state,
    };
    use crate::orchestration::LedgerWriter;
    use crate::orchestration::compactor::{
        DepResolution, DepSatisfactionRow, FoldState, MicroCompactor, RunRow,
        RunState as FoldRunState, TaskRow, TaskState as FoldTaskState,
    };
    use crate::orchestration::events::{
        OrchestrationEvent, OrchestrationEventData, OutputVisibilityState, TaskDef, TaskOutcome,
        TriggerInfo,
    };

    #[test]
    fn run_detail_from_projection_maps_cancel_requested_to_cancelling() {
        let mut state = FoldState::new();
        state.runs.insert(
            "run-cancel".to_string(),
            run_row("run-cancel", FoldRunState::Running, 1, 1_000, None, true),
        );
        state.tasks.insert(
            ("run-cancel".to_string(), "extract".to_string()),
            task_row("run-cancel", "extract", FoldTaskState::Running, 1),
        );

        let detail = run_detail_from_state(&state, "run-cancel");

        assert_eq!(
            detail,
            Some(RunDetail {
                run_id: "run-cancel".to_string(),
                plan_id: "plan-run-cancel".to_string(),
                state: RunStateView::Cancelling,
                created_at: ts(1_000),
                started_at: Some(ts(1_100)),
                completed_at: None,
                tasks: vec![super::TaskSummary {
                    task_key: "extract".to_string(),
                    asset_key: Some("asset.extract".to_string()),
                    state: TaskStateView::Running,
                    attempt: 1,
                    started_at: Some(ts(1_100)),
                    completed_at: None,
                    error_message: None,
                    execution_lineage_ref: None,
                    delta_table: None,
                    delta_version: None,
                    delta_partition: None,
                    retry_attribution: None,
                    skip_attribution: None,
                    output_visibility_state: None,
                    published_at: None,
                    publish_error: None,
                }],
                task_counts: super::TaskCounts {
                    total: 1,
                    pending: 0,
                    queued: 0,
                    running: 1,
                    succeeded: 0,
                    failed: 0,
                    skipped: 0,
                    cancelled: 0,
                },
                labels: HashMap::new(),
                parent_run_id: None,
                rerun_kind: None,
                rerun_reason: None,
            })
        );
    }

    #[test]
    fn run_list_from_projection_sorts_newest_first_and_filters_state() {
        let mut state = FoldState::new();
        state.runs.insert(
            "run-old".to_string(),
            run_row("run-old", FoldRunState::Triggered, 0, 1_000, None, false),
        );
        state.runs.insert(
            "run-output-pending".to_string(),
            run_row(
                "run-output-pending",
                FoldRunState::Succeeded,
                1,
                2_000,
                Some(2_500),
                false,
            ),
        );
        state.tasks.insert(
            ("run-output-pending".to_string(), "publish".to_string()),
            required_output_task(
                "run-output-pending",
                "publish",
                OutputVisibilityState::Pending,
            ),
        );
        state.runs.insert(
            "run-new".to_string(),
            run_row("run-new", FoldRunState::Running, 1, 3_000, None, false),
        );
        state.tasks.insert(
            ("run-new".to_string(), "extract".to_string()),
            task_row("run-new", "extract", FoldTaskState::Running, 1),
        );

        let page = list_runs_from_state(
            &state,
            &RunListQuery {
                limit: 10,
                cursor: None,
                state: Some(RunStateView::Running),
            },
        );

        assert_eq!(page.next_cursor, None);
        assert_eq!(
            page.runs
                .iter()
                .map(|run| (run.run_id.as_str(), run.state, run.completed_at))
                .collect::<Vec<_>>(),
            vec![
                ("run-new", RunStateView::Running, None),
                ("run-output-pending", RunStateView::Running, None),
            ]
        );
    }

    #[test]
    fn run_list_cursor_is_stable_when_newer_runs_are_inserted_between_pages() {
        let mut state = FoldState::new();
        state.runs.insert(
            "run-old".to_string(),
            run_row("run-old", FoldRunState::Triggered, 0, 1_000, None, false),
        );
        state.runs.insert(
            "run-mid".to_string(),
            run_row("run-mid", FoldRunState::Running, 1, 2_000, None, false),
        );
        state.runs.insert(
            "run-new".to_string(),
            run_row("run-new", FoldRunState::Running, 1, 3_000, None, false),
        );

        let first_page = list_runs_from_state(
            &state,
            &RunListQuery {
                limit: 1,
                cursor: None,
                state: None,
            },
        );
        assert_eq!(first_page.runs[0].run_id, "run-new");

        state.runs.insert(
            "run-later".to_string(),
            run_row("run-later", FoldRunState::Running, 1, 4_000, None, false),
        );

        let second_page = list_runs_from_state(
            &state,
            &RunListQuery {
                limit: 1,
                cursor: first_page.next_cursor,
                state: None,
            },
        );

        assert_eq!(
            second_page.runs[0].run_id, "run-mid",
            "cursor should resume after the previous page's last row, not after a shifted offset"
        );
    }

    #[test]
    fn run_detail_includes_retry_and_skip_attribution() {
        let mut state = FoldState::new();
        state.runs.insert(
            "run-attribution".to_string(),
            run_row(
                "run-attribution",
                FoldRunState::Failed,
                3,
                4_000,
                Some(4_500),
                false,
            ),
        );
        state.tasks.insert(
            ("run-attribution".to_string(), "retry".to_string()),
            task_row("run-attribution", "retry", FoldTaskState::Failed, 3),
        );
        state.tasks.insert(
            ("run-attribution".to_string(), "upstream-a".to_string()),
            task_row("run-attribution", "upstream-a", FoldTaskState::Failed, 1),
        );
        state.tasks.insert(
            ("run-attribution".to_string(), "skipped".to_string()),
            task_row("run-attribution", "skipped", FoldTaskState::Skipped, 1),
        );
        state.dep_satisfaction.insert(
            (
                "run-attribution".to_string(),
                "upstream-b".to_string(),
                "skipped".to_string(),
            ),
            dep_row(
                "run-attribution",
                "upstream-b",
                "skipped",
                DepResolution::Skipped,
                Some(4_200),
                "02",
            ),
        );
        state.dep_satisfaction.insert(
            (
                "run-attribution".to_string(),
                "upstream-a".to_string(),
                "skipped".to_string(),
            ),
            dep_row(
                "run-attribution",
                "upstream-a",
                "skipped",
                DepResolution::Failed,
                Some(4_100),
                "03",
            ),
        );
        state.dep_satisfaction.insert(
            (
                "run-attribution".to_string(),
                "upstream-c".to_string(),
                "skipped".to_string(),
            ),
            dep_row(
                "run-attribution",
                "upstream-c",
                "skipped",
                DepResolution::Success,
                Some(4_000),
                "01",
            ),
        );

        let detail = run_detail_from_state(&state, "run-attribution").unwrap();
        let retry = detail
            .tasks
            .iter()
            .find(|task| task.task_key == "retry")
            .unwrap();
        let skipped = detail
            .tasks
            .iter()
            .find(|task| task.task_key == "skipped")
            .unwrap();

        assert_eq!(
            retry.retry_attribution,
            Some(TaskRetryAttribution {
                retries: 2,
                reason: "prior_attempt_failed".to_string(),
            })
        );
        assert_eq!(
            skipped.skip_attribution,
            Some(TaskSkipAttribution {
                upstream_task_key: "upstream-a".to_string(),
                upstream_resolution: "FAILED".to_string(),
            })
        );
    }

    #[test]
    fn run_detail_maps_failed_required_output_to_failed() {
        let mut state = FoldState::new();
        state.runs.insert(
            "run-output-failed".to_string(),
            run_row(
                "run-output-failed",
                FoldRunState::Succeeded,
                1,
                4_000,
                Some(4_500),
                false,
            ),
        );
        state.tasks.insert(
            ("run-output-failed".to_string(), "publish".to_string()),
            required_output_task(
                "run-output-failed",
                "publish",
                OutputVisibilityState::Failed,
            ),
        );

        let detail = run_detail_from_state(&state, "run-output-failed").unwrap();

        assert_eq!(detail.state, RunStateView::Failed);
        assert_eq!(detail.completed_at, Some(ts(4_500)));
    }

    #[test]
    fn task_detail_lookup_requires_run_id_and_task_key() {
        let mut state = FoldState::new();
        state.runs.insert(
            "run-a".to_string(),
            run_row("run-a", FoldRunState::Failed, 1, 1_000, Some(1_500), false),
        );
        state.tasks.insert(
            ("run-a".to_string(), "extract".to_string()),
            task_row("run-a", "extract", FoldTaskState::Failed, 1),
        );
        state.runs.insert(
            "run-b".to_string(),
            run_row(
                "run-b",
                FoldRunState::Succeeded,
                1,
                2_000,
                Some(2_500),
                false,
            ),
        );
        state.tasks.insert(
            ("run-b".to_string(), "extract".to_string()),
            task_row("run-b", "extract", FoldTaskState::Succeeded, 2),
        );

        let detail = task_detail_from_state(&state, "run-b", "extract");

        assert_eq!(
            detail,
            Some(TaskDetail {
                run_id: "run-b".to_string(),
                task_key: "extract".to_string(),
                asset_key: Some("asset.extract".to_string()),
                partition_key: None,
                state: TaskStateView::Succeeded,
                attempt: 2,
                attempt_id: Some("attempt-run-b-extract-2".to_string()),
                started_at: Some(ts(2_100)),
                completed_at: Some(ts(2_400)),
                error_message: None,
                deps_total: 0,
                deps_satisfied_count: 0,
                max_attempts: 3,
                heartbeat_timeout_sec: 300,
                last_heartbeat_at: Some(ts(2_150)),
                ready_at: Some(ts(2_050)),
                requires_visible_output: false,
                materialization_id: None,
                output_visibility_state: None,
                published_at: None,
                publish_error: None,
                delta_table: None,
                delta_version: None,
                delta_partition: None,
                execution_lineage_ref: None,
            })
        );
        assert_eq!(
            task_detail_from_state(&state, "missing-run", "extract"),
            None
        );
    }

    #[tokio::test]
    async fn state_service_load_snapshot_reads_compacted_projection() {
        let storage = test_storage();
        let manifest = seed_events(&storage, active_run_events(&storage, "run-visible")).await;
        let service = OrchestrationStateService::new(storage);

        let snapshot = service.load_snapshot().await.expect("load snapshot");

        assert_eq!(snapshot.manifest_id, manifest.manifest_id);
        assert_eq!(snapshot.manifest_revision, manifest.revision_ulid);
        assert_eq!(snapshot.runs.len(), 1);
        assert_eq!(snapshot.runs[0].run_id, "run-visible");

        let run = service
            .get_run("run-visible")
            .await
            .expect("get run")
            .expect("run exists");
        assert_eq!(run.state, RunStateView::Running);

        let page = service
            .list_runs(RunListQuery {
                limit: 10,
                cursor: None,
                state: Some(RunStateView::Running),
            })
            .await
            .expect("list runs");
        assert_eq!(page.runs.len(), 1);
        assert_eq!(page.runs[0].run_id, "run-visible");

        let task = service
            .get_task("run-visible", "extract")
            .await
            .expect("get task")
            .expect("task exists");
        assert_eq!(task.state, TaskStateView::Queued);
    }

    #[tokio::test]
    async fn state_service_cancel_before_start_keeps_dispatched_task_cancelled() {
        let storage = test_storage();
        let run_id = "run-cancel-before-start";
        let attempt_id = attempt_id(run_id, "extract", 1);
        let mut events = active_run_events(&storage, run_id);
        events.push(dispatch_requested_event(
            &storage,
            run_id,
            "extract",
            1,
            &attempt_id,
            3,
        ));
        events.push(run_cancel_requested_event(
            &storage,
            run_id,
            Some("operator_requested"),
            4,
        ));
        events.push(task_started_event(
            &storage,
            run_id,
            "extract",
            1,
            &attempt_id,
            5,
        ));
        seed_events(&storage, events).await;
        let service = OrchestrationStateService::new(storage);

        let task = service
            .get_task(run_id, "extract")
            .await
            .expect("get task")
            .expect("task exists");
        assert_eq!(task.state, TaskStateView::Cancelled);
        assert_eq!(task.attempt, 1);
        assert_eq!(task.attempt_id.as_deref(), Some(attempt_id.as_str()));
        assert_eq!(task.started_at, None);

        let run = service
            .get_run(run_id)
            .await
            .expect("get run")
            .expect("run exists");
        assert_eq!(run.state, RunStateView::Cancelled);
        assert_eq!(run.task_counts.cancelled, 1);
        assert_eq!(run.task_counts.running, 0);
    }

    #[tokio::test]
    async fn state_service_stale_finish_events_do_not_overwrite_succeeded_task() {
        let storage = test_storage();
        let run_id = "run-stale-finish";
        let attempt_1_id = attempt_id(run_id, "extract", 1);
        let attempt_2_id = attempt_id(run_id, "extract", 2);
        let mut events = active_run_events(&storage, run_id);
        events.push(task_started_event(
            &storage,
            run_id,
            "extract",
            1,
            &attempt_1_id,
            3,
        ));
        events.push(task_started_event(
            &storage,
            run_id,
            "extract",
            2,
            &attempt_2_id,
            4,
        ));
        events.push(task_finished_event(
            &storage,
            run_id,
            "extract",
            2,
            &attempt_2_id,
            5,
        ));
        events.push(task_finished_event_with_outcome(
            &storage,
            run_id,
            "extract",
            1,
            &attempt_1_id,
            TaskOutcome::Failed,
            6,
        ));
        events.push(task_finished_event_with_outcome_and_idempotency_key(
            &storage,
            run_id,
            "extract",
            2,
            &attempt_2_id,
            TaskOutcome::Failed,
            "late-conflicting-finish:run-stale-finish:extract:2",
            7,
        ));
        seed_events(&storage, events).await;
        let service = OrchestrationStateService::new(storage);

        let task = service
            .get_task(run_id, "extract")
            .await
            .expect("get task")
            .expect("task exists");
        assert_eq!(task.state, TaskStateView::Succeeded);
        assert_eq!(task.attempt, 2);
        assert_eq!(task.attempt_id.as_deref(), Some(attempt_2_id.as_str()));
        assert_eq!(task.completed_at, Some(ts(10_005)));
        assert_eq!(task.error_message, None);

        let run = service
            .get_run(run_id)
            .await
            .expect("get run")
            .expect("run exists");
        assert_eq!(run.state, RunStateView::Succeeded);
        assert_eq!(run.task_counts.succeeded, 1);
        assert_eq!(run.task_counts.failed, 0);
    }

    #[tokio::test]
    async fn state_service_get_task_scopes_same_task_key_by_run_id() {
        let storage = test_storage();
        let mut events = terminal_run_events(&storage, "run-same-key-a");
        events.extend(active_run_events(&storage, "run-same-key-b"));
        seed_events(&storage, events).await;
        let service = OrchestrationStateService::new(storage);

        let run_a_task = service
            .get_task("run-same-key-a", "extract")
            .await
            .expect("get run a task")
            .expect("run a task exists");
        let run_b_task = service
            .get_task("run-same-key-b", "extract")
            .await
            .expect("get run b task")
            .expect("run b task exists");

        assert_eq!(run_a_task.run_id, "run-same-key-a");
        assert_eq!(run_a_task.state, TaskStateView::Succeeded);
        assert_eq!(run_b_task.run_id, "run-same-key-b");
        assert_eq!(run_b_task.state, TaskStateView::Queued);
    }

    #[tokio::test]
    async fn state_service_cancel_run_appends_visible_cancel_event() {
        let storage = test_storage();
        seed_events(&storage, active_run_events(&storage, "run-cancel-visible")).await;
        let service = OrchestrationStateService::new(storage.clone());
        let before_count = ledger_event_count(&storage).await;

        let outcome = service
            .cancel_run(CancelRunRequest {
                run_id: "run-cancel-visible".to_string(),
                reason: Some("operator_requested".to_string()),
                requested_by: "operator@example.com".to_string(),
            })
            .await
            .expect("cancel run");

        assert_eq!(outcome.disposition, CancelRunDisposition::Requested);
        assert_eq!(outcome.run_id, "run-cancel-visible");
        assert_eq!(outcome.state, RunStateView::Cancelled);
        assert_eq!(ledger_event_count(&storage).await, before_count + 1);

        let run = service
            .get_run("run-cancel-visible")
            .await
            .expect("get run")
            .expect("run exists");
        assert_eq!(run.state, RunStateView::Cancelled);
    }

    #[tokio::test]
    async fn state_service_cancel_run_is_idempotent_when_cancel_already_requested() {
        let storage = test_storage();
        seed_events(
            &storage,
            running_cancel_requested_run_events(&storage, "run-cancel-idempotent"),
        )
        .await;
        let service = OrchestrationStateService::new(storage.clone());
        let before_count = ledger_event_count(&storage).await;

        let outcome = service
            .cancel_run(CancelRunRequest {
                run_id: "run-cancel-idempotent".to_string(),
                reason: Some("duplicate_request".to_string()),
                requested_by: "operator@example.com".to_string(),
            })
            .await
            .expect("cancel run");

        assert_eq!(outcome.disposition, CancelRunDisposition::AlreadyRequested);
        assert_eq!(outcome.state, RunStateView::Cancelling);
        assert_eq!(ledger_event_count(&storage).await, before_count);
    }

    #[tokio::test]
    async fn state_service_cancel_run_is_idempotent_after_cancel_reaches_terminal_state() {
        let storage = test_storage();
        seed_events(&storage, active_run_events(&storage, "run-cancel-terminal")).await;
        let service = OrchestrationStateService::new(storage.clone());
        let first_count = ledger_event_count(&storage).await;

        let first = service
            .cancel_run(CancelRunRequest {
                run_id: "run-cancel-terminal".to_string(),
                reason: Some("operator_requested".to_string()),
                requested_by: "operator@example.com".to_string(),
            })
            .await
            .expect("first cancel");
        assert_eq!(first.disposition, CancelRunDisposition::Requested);
        assert_eq!(first.state, RunStateView::Cancelled);
        assert_eq!(ledger_event_count(&storage).await, first_count + 1);

        let second_count = ledger_event_count(&storage).await;
        let second = service
            .cancel_run(CancelRunRequest {
                run_id: "run-cancel-terminal".to_string(),
                reason: Some("duplicate_request".to_string()),
                requested_by: "operator@example.com".to_string(),
            })
            .await
            .expect("second cancel should be idempotent");

        assert_eq!(second.disposition, CancelRunDisposition::AlreadyRequested);
        assert_eq!(second.state, RunStateView::Cancelled);
        assert_eq!(ledger_event_count(&storage).await, second_count);
    }

    #[tokio::test]
    async fn state_service_cancel_run_observes_terminal_state_after_lock_wait() {
        let storage = test_storage();
        seed_events(&storage, active_run_events(&storage, "run-lock-race")).await;
        let lock_path = crate::orchestration_compaction_lock_path();
        let lock = DistributedLock::new(storage.backend().clone(), lock_path);
        let guard = lock
            .acquire(DEFAULT_LOCK_TTL, 10)
            .await
            .expect("hold compaction lock");

        let terminal_events = terminal_task_transition_events(&storage, "run-lock-race", 3);
        append_events_and_compact_with_guard(&storage, terminal_events, &guard, lock_path).await;
        let terminal_event_count = ledger_event_count(&storage).await;
        guard.release().await.expect("release compaction lock");

        let error = OrchestrationStateService::new(storage.clone())
            .cancel_run(CancelRunRequest {
                run_id: "run-lock-race".to_string(),
                reason: Some("stale_request".to_string()),
                requested_by: "operator@example.com".to_string(),
            })
            .await
            .expect_err("cancel should observe terminal state before appending");

        assert!(matches!(
            error,
            StateServiceError::TerminalRun {
                run_id,
                state: RunStateView::Succeeded,
            } if run_id == "run-lock-race"
        ));
        assert_eq!(
            ledger_event_count(&storage).await,
            terminal_event_count,
            "cancel request must not append after terminal state wins the lock"
        );
    }

    #[tokio::test]
    async fn state_service_cancel_run_rejects_terminal_run() {
        let storage = test_storage();
        seed_events(&storage, terminal_run_events(&storage, "run-terminal")).await;
        let service = OrchestrationStateService::new(storage);

        let error = service
            .cancel_run(CancelRunRequest {
                run_id: "run-terminal".to_string(),
                reason: Some("too_late".to_string()),
                requested_by: "operator@example.com".to_string(),
            })
            .await
            .expect_err("terminal run should reject cancellation");

        assert!(matches!(
            error,
            StateServiceError::TerminalRun {
                run_id,
                state: RunStateView::Succeeded,
            } if run_id == "run-terminal"
        ));
    }

    #[tokio::test]
    async fn state_service_cancel_run_reports_internal_terminal_state_for_output_pending_run() {
        let storage = test_storage();
        let run_id = "run-output-pending-terminal";
        let attempt_id = attempt_id(run_id, "publish", 1);
        seed_events(
            &storage,
            vec![
                run_triggered_event(&storage, run_id, 1),
                plan_created_event(
                    &storage,
                    run_id,
                    vec![required_output_task_def("publish", vec![])],
                    2,
                ),
                task_started_event(&storage, run_id, "publish", 1, &attempt_id, 3),
                task_finished_event(&storage, run_id, "publish", 1, &attempt_id, 4),
                task_output_visibility_event(
                    &storage,
                    run_id,
                    "publish",
                    1,
                    &attempt_id,
                    OutputVisibilityState::Pending,
                    5,
                ),
            ],
        )
        .await;
        let service = OrchestrationStateService::new(storage);

        let run = service
            .get_run(run_id)
            .await
            .expect("get run")
            .expect("run exists");
        assert_eq!(run.state, RunStateView::Running);

        let error = service
            .cancel_run(CancelRunRequest {
                run_id: run_id.to_string(),
                reason: Some("too_late".to_string()),
                requested_by: "operator@example.com".to_string(),
            })
            .await
            .expect_err("internally terminal run should reject cancellation");

        assert!(matches!(
            error,
            StateServiceError::TerminalRun {
                run_id: error_run_id,
                state: RunStateView::Succeeded,
            } if error_run_id == run_id
        ));
    }

    #[tokio::test]
    async fn state_service_cancel_run_returns_not_found_for_missing_run() {
        let storage = test_storage();
        let service = OrchestrationStateService::new(storage);

        let error = service
            .cancel_run(CancelRunRequest {
                run_id: "missing-run".to_string(),
                reason: None,
                requested_by: "operator@example.com".to_string(),
            })
            .await
            .expect_err("missing run should return not found");

        assert!(matches!(
            error,
            StateServiceError::RunNotFound { run_id } if run_id == "missing-run"
        ));
    }

    #[tokio::test]
    async fn state_service_list_runs_rejects_api_invalid_queries() {
        let storage = test_storage();
        let service = OrchestrationStateService::new(storage).with_max_run_list_limit(2);

        let zero_limit = service
            .list_runs(RunListQuery {
                limit: 0,
                cursor: None,
                state: None,
            })
            .await
            .expect_err("zero limit should be rejected");
        assert!(matches!(
            zero_limit,
            StateServiceError::ZeroRunListLimit { max: 2 }
        ));

        let too_large = service
            .list_runs(RunListQuery {
                limit: 3,
                cursor: None,
                state: None,
            })
            .await
            .expect_err("limit above max should be rejected");
        assert!(matches!(
            too_large,
            StateServiceError::RunListLimitTooLarge { limit: 3, max: 2 }
        ));

        let invalid_cursor = service
            .list_runs(RunListQuery {
                limit: 1,
                cursor: Some("not-a-cursor".to_string()),
                state: None,
            })
            .await
            .expect_err("invalid cursor should be rejected");
        assert!(matches!(
            invalid_cursor,
            StateServiceError::InvalidRunListCursor { cursor } if cursor == "not-a-cursor"
        ));
    }

    #[test]
    fn state_service_options_configure_remote_compaction_and_request_id() {
        let service = OrchestrationStateService::new(test_storage())
            .with_orchestration_compactor_url("https://compactor.example")
            .with_request_id("req-123")
            .with_max_run_list_limit(50);

        assert_eq!(
            service.options.orchestration_compactor_url.as_deref(),
            Some("https://compactor.example")
        );
        assert_eq!(service.options.request_id.as_deref(), Some("req-123"));
        assert_eq!(service.options.max_run_list_limit, 50);
    }

    fn run_row(
        run_id: &str,
        state: FoldRunState,
        tasks_total: u32,
        triggered_at: i64,
        completed_at: Option<i64>,
        cancel_requested: bool,
    ) -> RunRow {
        RunRow {
            run_id: run_id.to_string(),
            plan_id: format!("plan-{run_id}"),
            state,
            run_key: None,
            labels: HashMap::new(),
            code_version: None,
            cancel_requested,
            tasks_total,
            tasks_completed: if state.is_terminal() { tasks_total } else { 0 },
            tasks_succeeded: if state == FoldRunState::Succeeded {
                tasks_total
            } else {
                0
            },
            tasks_failed: if state == FoldRunState::Failed {
                tasks_total
            } else {
                0
            },
            tasks_skipped: 0,
            tasks_cancelled: if state == FoldRunState::Cancelled {
                tasks_total
            } else {
                0
            },
            triggered_at: ts(triggered_at),
            completed_at: completed_at.map(ts),
            row_version: format!("version-{run_id}"),
        }
    }

    fn task_row(run_id: &str, task_key: &str, state: FoldTaskState, attempt: u32) -> TaskRow {
        let terminal = state.is_terminal();
        TaskRow {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            state,
            attempt,
            attempt_id: Some(format!("attempt-{run_id}-{task_key}-{attempt}")),
            started_at: Some(ts(run_base(run_id) + 100)),
            completed_at: terminal.then(|| ts(run_base(run_id) + 400)),
            error_message: (state == FoldTaskState::Failed).then(|| "task failed".to_string()),
            deps_total: 0,
            deps_satisfied_count: 0,
            max_attempts: 3,
            heartbeat_timeout_sec: 300,
            last_heartbeat_at: Some(ts(run_base(run_id) + 150)),
            ready_at: Some(ts(run_base(run_id) + 50)),
            asset_key: Some(format!("asset.{task_key}")),
            partition_key: None,
            requires_visible_output: false,
            materialization_id: None,
            output_visibility_state: None,
            published_at: None,
            publish_error: None,
            retry_not_before: None,
            delta_table: None,
            delta_version: None,
            delta_partition: None,
            execution_lineage_ref: None,
            row_version: format!("version-{run_id}-{task_key}"),
        }
    }

    fn required_output_task(
        run_id: &str,
        task_key: &str,
        visibility_state: OutputVisibilityState,
    ) -> TaskRow {
        TaskRow {
            requires_visible_output: true,
            output_visibility_state: Some(visibility_state),
            ..task_row(run_id, task_key, FoldTaskState::Succeeded, 1)
        }
    }

    fn dep_row(
        run_id: &str,
        upstream_task_key: &str,
        downstream_task_key: &str,
        resolution: DepResolution,
        satisfied_at: Option<i64>,
        row_version: &str,
    ) -> DepSatisfactionRow {
        DepSatisfactionRow {
            run_id: run_id.to_string(),
            upstream_task_key: upstream_task_key.to_string(),
            downstream_task_key: downstream_task_key.to_string(),
            satisfied: true,
            resolution: Some(resolution),
            satisfied_at: satisfied_at.map(ts),
            satisfying_attempt: Some(1),
            row_version: row_version.to_string(),
        }
    }

    fn run_base(run_id: &str) -> i64 {
        match run_id {
            "run-a" | "run-cancel" => 1_000,
            "run-b" | "run-output-pending" => 2_000,
            "run-new" => 3_000,
            _ => 4_000,
        }
    }

    fn ts(seconds: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().unwrap()
    }

    fn test_storage() -> ScopedStorage {
        let backend = Arc::new(MemoryBackend::new());
        ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage")
    }

    async fn seed_events(
        storage: &ScopedStorage,
        events: Vec<OrchestrationEvent>,
    ) -> crate::orchestration::compactor::OrchestrationManifest {
        let ledger = LedgerWriter::new(storage.clone());
        let event_paths = events
            .iter()
            .map(LedgerWriter::event_path)
            .collect::<Vec<_>>();
        ledger.append_all(events).await.expect("append events");
        let result = MicroCompactor::new(storage.clone())
            .compact_events(event_paths)
            .await
            .expect("compact events");
        assert_eq!(result.visibility_status.as_str(), "visible");

        let (manifest, _) = MicroCompactor::new(storage.clone())
            .load_state()
            .await
            .expect("load compacted state");
        manifest
    }

    async fn append_events_and_compact_with_guard(
        storage: &ScopedStorage,
        events: Vec<OrchestrationEvent>,
        guard: &arco_core::LockGuard<dyn arco_core::storage::StorageBackend>,
        lock_path: &str,
    ) {
        let ledger = LedgerWriter::new(storage.clone());
        let event_paths = events
            .iter()
            .map(LedgerWriter::event_path)
            .collect::<Vec<_>>();
        ledger.append_all(events).await.expect("append events");
        let result = MicroCompactor::new(storage.clone())
            .compact_events_fenced(event_paths, guard.fencing_token().sequence(), lock_path)
            .await
            .expect("compact events");
        assert_eq!(result.visibility_status.as_str(), "visible");
    }

    async fn ledger_event_count(storage: &ScopedStorage) -> usize {
        storage
            .list("ledger/orchestration")
            .await
            .expect("list ledger")
            .len()
    }

    fn active_run_events(storage: &ScopedStorage, run_id: &str) -> Vec<OrchestrationEvent> {
        vec![
            run_triggered_event(storage, run_id, 1),
            plan_created_event(storage, run_id, vec![task_def("extract", vec![])], 2),
        ]
    }

    fn running_cancel_requested_run_events(
        storage: &ScopedStorage,
        run_id: &str,
    ) -> Vec<OrchestrationEvent> {
        let attempt_id = attempt_id(run_id, "extract", 1);
        vec![
            run_triggered_event(storage, run_id, 1),
            plan_created_event(
                storage,
                run_id,
                vec![
                    task_def("extract", vec![]),
                    task_def("load", vec!["extract"]),
                ],
                2,
            ),
            task_started_event(storage, run_id, "extract", 1, &attempt_id, 3),
            run_cancel_requested_event(storage, run_id, Some("already_requested"), 4),
        ]
    }

    fn terminal_run_events(storage: &ScopedStorage, run_id: &str) -> Vec<OrchestrationEvent> {
        let attempt_id = attempt_id(run_id, "extract", 1);
        vec![
            run_triggered_event(storage, run_id, 1),
            plan_created_event(storage, run_id, vec![task_def("extract", vec![])], 2),
            task_started_event(storage, run_id, "extract", 1, &attempt_id, 3),
            task_finished_event(storage, run_id, "extract", 1, &attempt_id, 4),
        ]
    }

    fn terminal_task_transition_events(
        storage: &ScopedStorage,
        run_id: &str,
        first_timestamp_offset_seconds: i64,
    ) -> Vec<OrchestrationEvent> {
        let attempt_id = attempt_id(run_id, "extract", 1);
        vec![
            task_started_event(
                storage,
                run_id,
                "extract",
                1,
                &attempt_id,
                first_timestamp_offset_seconds,
            ),
            task_finished_event(
                storage,
                run_id,
                "extract",
                1,
                &attempt_id,
                first_timestamp_offset_seconds + 1,
            ),
        ]
    }

    fn run_triggered_event(
        storage: &ScopedStorage,
        run_id: &str,
        timestamp_offset_seconds: i64,
    ) -> OrchestrationEvent {
        event(
            storage,
            OrchestrationEventData::RunTriggered {
                run_id: run_id.to_string(),
                plan_id: format!("plan-{run_id}"),
                trigger: TriggerInfo::Manual {
                    user_id: "operator@example.com".to_string(),
                },
                root_assets: vec!["asset.extract".to_string()],
                run_key: None,
                labels: HashMap::new(),
                code_version: None,
            },
            timestamp_offset_seconds,
        )
    }

    fn plan_created_event(
        storage: &ScopedStorage,
        run_id: &str,
        tasks: Vec<TaskDef>,
        timestamp_offset_seconds: i64,
    ) -> OrchestrationEvent {
        event(
            storage,
            OrchestrationEventData::PlanCreated {
                run_id: run_id.to_string(),
                plan_id: format!("plan-{run_id}"),
                tasks,
            },
            timestamp_offset_seconds,
        )
    }

    fn task_started_event(
        storage: &ScopedStorage,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        timestamp_offset_seconds: i64,
    ) -> OrchestrationEvent {
        event(
            storage,
            OrchestrationEventData::TaskStarted {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt,
                attempt_id: attempt_id.to_string(),
                worker_id: "worker-01".to_string(),
            },
            timestamp_offset_seconds,
        )
    }

    fn dispatch_requested_event(
        storage: &ScopedStorage,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        timestamp_offset_seconds: i64,
    ) -> OrchestrationEvent {
        event(
            storage,
            OrchestrationEventData::DispatchRequested {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt,
                attempt_id: attempt_id.to_string(),
                worker_queue: "default-queue".to_string(),
                dispatch_id: format!("dispatch-{run_id}-{task_key}-{attempt}"),
            },
            timestamp_offset_seconds,
        )
    }

    fn task_finished_event(
        storage: &ScopedStorage,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        timestamp_offset_seconds: i64,
    ) -> OrchestrationEvent {
        task_finished_event_with_outcome(
            storage,
            run_id,
            task_key,
            attempt,
            attempt_id,
            TaskOutcome::Succeeded,
            timestamp_offset_seconds,
        )
    }

    fn task_finished_event_with_outcome(
        storage: &ScopedStorage,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        outcome: TaskOutcome,
        timestamp_offset_seconds: i64,
    ) -> OrchestrationEvent {
        event(
            storage,
            OrchestrationEventData::TaskFinished {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt,
                attempt_id: attempt_id.to_string(),
                worker_id: "worker-01".to_string(),
                outcome,
                materialization_id: None,
                error_message: (outcome == TaskOutcome::Failed)
                    .then(|| "late worker failure".to_string()),
                output: None,
                error: None,
                metrics: None,
                cancelled_during_phase: None,
                partial_progress_json: None,
                asset_key: Some(format!("asset.{task_key}")),
                partition_key: None,
                code_version: None,
            },
            timestamp_offset_seconds,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn task_finished_event_with_outcome_and_idempotency_key(
        storage: &ScopedStorage,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        outcome: TaskOutcome,
        idempotency_key: &str,
        timestamp_offset_seconds: i64,
    ) -> OrchestrationEvent {
        let mut event = OrchestrationEvent::new_with_timestamp_and_idempotency_key(
            storage.tenant_id(),
            storage.workspace_id(),
            OrchestrationEventData::TaskFinished {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt,
                attempt_id: attempt_id.to_string(),
                worker_id: "worker-01".to_string(),
                outcome,
                materialization_id: None,
                error_message: (outcome == TaskOutcome::Failed)
                    .then(|| "late worker failure".to_string()),
                output: None,
                error: None,
                metrics: None,
                cancelled_during_phase: None,
                partial_progress_json: None,
                asset_key: Some(format!("asset.{task_key}")),
                partition_key: None,
                code_version: None,
            },
            idempotency_key,
            ts(10_000 + timestamp_offset_seconds),
        );
        event.event_id = test_event_id(timestamp_offset_seconds, &event.idempotency_key);
        event
    }

    fn task_output_visibility_event(
        storage: &ScopedStorage,
        run_id: &str,
        task_key: &str,
        attempt: u32,
        attempt_id: &str,
        visibility_state: OutputVisibilityState,
        timestamp_offset_seconds: i64,
    ) -> OrchestrationEvent {
        event(
            storage,
            OrchestrationEventData::TaskOutputVisibilityChanged {
                run_id: run_id.to_string(),
                task_key: task_key.to_string(),
                attempt,
                attempt_id: attempt_id.to_string(),
                visibility_state,
                published_at: None,
                publish_error: None,
            },
            timestamp_offset_seconds,
        )
    }

    fn run_cancel_requested_event(
        storage: &ScopedStorage,
        run_id: &str,
        reason: Option<&str>,
        timestamp_offset_seconds: i64,
    ) -> OrchestrationEvent {
        event(
            storage,
            OrchestrationEventData::RunCancelRequested {
                run_id: run_id.to_string(),
                reason: reason.map(ToString::to_string),
                requested_by: "operator@example.com".to_string(),
            },
            timestamp_offset_seconds,
        )
    }

    fn event(
        storage: &ScopedStorage,
        data: OrchestrationEventData,
        timestamp_offset_seconds: i64,
    ) -> OrchestrationEvent {
        let mut event = OrchestrationEvent::new_with_timestamp(
            storage.tenant_id(),
            storage.workspace_id(),
            data,
            ts(10_000 + timestamp_offset_seconds),
        );
        event.event_id = test_event_id(timestamp_offset_seconds, &event.idempotency_key);
        event
    }

    fn test_event_id(timestamp_offset_seconds: i64, discriminator: &str) -> String {
        let suffix = discriminator.bytes().fold(0_u64, |acc, byte| {
            acc.wrapping_mul(131).wrapping_add(u64::from(byte))
        }) % 1_000_000_000_000;
        format!("01KSTATE{timestamp_offset_seconds:06}{suffix:012}")
    }

    fn task_def(key: &str, depends_on: Vec<&str>) -> TaskDef {
        TaskDef {
            key: key.to_string(),
            depends_on: depends_on.into_iter().map(ToString::to_string).collect(),
            asset_key: Some(format!("asset.{key}")),
            partition_key: None,
            max_attempts: 3,
            heartbeat_timeout_sec: 300,
            requires_visible_output: false,
        }
    }

    fn required_output_task_def(key: &str, depends_on: Vec<&str>) -> TaskDef {
        TaskDef {
            requires_visible_output: true,
            ..task_def(key, depends_on)
        }
    }

    fn attempt_id(run_id: &str, task_key: &str, attempt: u32) -> String {
        format!("attempt-{run_id}-{task_key}-{attempt}")
    }
}
