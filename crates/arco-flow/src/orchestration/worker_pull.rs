//! Flow worker pulls for one fresh, exclusively operated workspace.
//!
//! The selected manifest is the acceptance log and projection for this profile.
//! The existing versioned pointer selects callback facts and their permit updates
//! together. A raw ledger append, unselected snapshot or push callback carries
//! no authority here.

mod advance;
mod callbacks;
mod http;
#[cfg(test)]
mod tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use arco_core::{DistributedLock, ScopedStorage, lock::DEFAULT_LOCK_TTL};
use arco_worker_contract::WorkerDispatchEnvelope;
use chrono::{DateTime, TimeDelta, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::time::Instant as TokioInstant;
use ulid::Ulid;

use super::{
    accepted_plan::{
        ACCEPTED_PLAN_SHA256_LABEL, AcceptedRunPlan, RETRY_POLICY_LABEL, RIVERS_RETRY_PROFILE,
        load_accepted_run_plan, populate_accepted_payload,
    },
    compactor::{
        DispatchOutboxRow, DispatchStatus, FoldState, MicroCompactor, OrchestrationManifest,
        TaskRow, TaskState, service::DeltaTombstoneMode,
    },
    events::{OrchestrationEvent, OrchestrationEventData},
    state::{CancelRunDisposition, CancelRunOutcome, CancelRunRequest, run_detail_from_state},
    worker_contract::{DispatchEnvelopeSpec, dispatch_envelope_for_attempt},
};
use crate::{error::Error, orchestration_compaction_lock_path};

const MAX_TASKS: usize = 128;
const MAX_PERMITS: usize = 8;
const MAX_RECEIPTS: usize = 1024;
const MAX_ADVANCE_ITEMS: usize = 8;
const DEDUP_HORIZON: TimeDelta = TimeDelta::hours(24);
const CALLBACK_TOKEN_TTL: TimeDelta = TimeDelta::hours(24);
const START_LEASE: TimeDelta = TimeDelta::seconds(15);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const LOCK_RELEASE_RESERVE: Duration = Duration::from_millis(500);
pub(crate) const PULL_RECHECK: &str = "worker-pull proposal must be re-evaluated";

type Clock = Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;
type PullResult<T> = Result<T, PullError>;

/// Configuration for an explicitly scoped, operator-owned pull host.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerPullConfig {
    /// Maximum simultaneous execution authorizations.
    pub permits: usize,
    /// Base URL for the host's existing worker callback paths.
    pub callback_base_url: String,
    /// Stable secret used to derive claim capabilities across host restarts.
    pub claim_key: [u8; 32],
    /// Bearer credential for manual control invocations.
    pub control_token: String,
    /// Worker session IDs and their distinct bearer credentials.
    pub sessions: BTreeMap<String, String>,
}

/// Outcome of a scoped pull operation.
#[derive(Debug, thiserror::Error)]
pub enum PullError {
    /// An input was outside the local profile.
    #[error("{0}")]
    Invalid(&'static str),
    /// A session or capability failed authentication.
    #[error("invalid worker-pull credential")]
    Unauthorized,
    /// The requested run is absent from the selected projection.
    #[error("run not found")]
    RunNotFound,
    /// Request identity or current claim prevents the transition.
    #[error("{0}")]
    Conflict(&'static str),
    /// Work may safely be retried.
    #[error("{0}")]
    Retryable(&'static str),
    /// The selected authority could not be read or updated.
    #[error(transparent)]
    Authority(#[from] Error),
}

impl From<arco_core::Error> for PullError {
    fn from(error: arco_core::Error) -> Self {
        Self::Authority(error.into())
    }
}

/// Exact grant read from the selected Flow manifest.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimGrant {
    /// Monotone authorization generation within the workspace.
    pub generation: u64,
    /// Assigned permit slot.
    pub permit: usize,
    /// Original start lease deadline.
    pub start_before: DateTime<Utc>,
    /// Current lease expiry read from the selected receipt.
    pub lease_expires_at: DateTime<Utc>,
    /// Unmodified worker dispatch wire format.
    pub envelope: WorkerDispatchEnvelope,
}

/// The result of a one-task pull or its replay.
#[derive(Debug, Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum ClaimReply {
    /// This request owns the selected claim.
    Granted {
        /// Original grant for this request.
        grant: Box<ClaimGrant>,
    },
    /// A pre-start or running authorization expired.
    Expired,
    /// A later generation owns the same dispatch.
    Superseded,
    /// Completion was recorded and the permit was released.
    Completed {
        /// Generation whose completion was accepted.
        generation: u64,
        /// Accepted worker outcome.
        final_state: String,
        /// Server time persisted with the accepted completion.
        accepted_at: DateTime<Utc>,
    },
    /// Visible run cancellation ended the claim.
    Cancelled,
    /// All permits are assigned. This observation does not consume the request ID.
    Busy,
    /// No eligible work exists. This observation does not consume the request ID.
    Empty,
}

/// Result of one bounded control invocation.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdvanceReply {
    /// Revision read back from the visible orchestration pointer.
    pub observed_revision: String,
    /// Expirations, releases and dispatches performed in this invocation.
    pub advanced: usize,
    /// Another step can make progress now.
    pub immediate_work: bool,
    /// Earliest outstanding lease or retry deadline.
    pub next_deadline: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Phase {
    Active,
    Expired,
    Superseded,
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Completion {
    digest: String,
    state: String,
    accepted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Receipt {
    dispatch: DispatchOutboxRow,
    session: String,
    generation: u64,
    permit: usize,
    granted_at: DateTime<Utc>,
    start_before: DateTime<Utc>,
    deadline: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    completion: Option<Completion>,
    ended_at: Option<DateTime<Utc>>,
    phase: Phase,
}

impl Receipt {
    fn task<'a>(&self, fold: &'a FoldState) -> Option<&'a TaskRow> {
        let row = &self.dispatch;
        fold.tasks.get(&(row.run_id.clone(), row.task_key.clone()))
    }

    fn matches_task(&self, task: &TaskRow) -> bool {
        task.attempt == self.dispatch.attempt
            && task.attempt_id.as_deref() == Some(&self.dispatch.attempt_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AcceptedIdentity {
    plan_id: String,
    sha256: String,
    key: String,
    tasks: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PullState {
    config_id: String,
    permits: Vec<Option<u64>>,
    last_generation: u64,
    accepted_runs: BTreeMap<String, AcceptedIdentity>,
    receipts: BTreeMap<String, Receipt>,
    // Exact events accepted by this immutable successor.
    pub(crate) events: Vec<OrchestrationEvent>,
}

impl PullState {
    pub(crate) fn uses_rivers_retry(&self, run_id: &str) -> bool {
        self.accepted_runs
            .get(run_id)
            .is_some_and(|plan| plan.retry_policy.as_deref() == Some(RIVERS_RETRY_PROFILE))
    }

    fn validate(&self, fold: &FoldState) -> PullResult<()> {
        let mut assigned = BTreeSet::new();
        for receipt in self
            .receipts
            .values()
            .filter(|receipt| receipt.phase == Phase::Active)
        {
            let task = receipt.task(fold);
            let run = fold.runs.get(&receipt.dispatch.run_id);
            let matches_task = task.is_some_and(|task| {
                receipt.matches_task(task)
                    && if receipt.started_at.is_some() {
                        task.state == TaskState::Running
                    } else {
                        task.state == TaskState::Dispatched
                            || (task.state == TaskState::Cancelled
                                && run.is_some_and(|run| run.cancel_requested))
                    }
            });
            if run.is_none()
                || !matches_task
                || !assigned.insert(receipt.generation)
                || receipt.generation > self.last_generation
                || self.permits.get(receipt.permit) != Some(&Some(receipt.generation))
            {
                return Err(PullError::Retryable(
                    "selected claim, permit and attempt diverged",
                ));
            }
        }
        if assigned.len() != self.permits.iter().flatten().count() {
            return Err(PullError::Retryable(
                "selected permit has no matching claim",
            ));
        }
        if fold.tasks.values().any(|task| {
            task.state == TaskState::RetryWait
                && task.retry_not_before.is_none()
                && self.uses_rivers_retry(&task.run_id)
        }) {
            return Err(PullError::Retryable(
                "selected Rivers retry has no deadline",
            ));
        }
        Ok(())
    }

    fn release(&mut self, request: &str, phase: Phase, now: DateTime<Utc>) -> PullResult<()> {
        let receipt = self
            .receipts
            .get_mut(request)
            .ok_or(PullError::Retryable("missing claim receipt"))?;
        let slot = self
            .permits
            .get_mut(receipt.permit)
            .filter(|slot| **slot == Some(receipt.generation) && receipt.phase == Phase::Active)
            .ok_or(PullError::Retryable("claim and execution permit diverged"))?;
        *slot = None;
        receipt.phase = phase;
        receipt.ended_at = Some(now);
        Ok(())
    }
}

pub(crate) struct PullUpdate<'a> {
    pub(crate) state: PullState,
    pub(crate) events: Vec<OrchestrationEvent>,
    pub(crate) expected_revision: Option<String>,
    pub(crate) preloaded_projection: Option<(&'a OrchestrationManifest, &'a FoldState)>,
    pub(crate) valid_before: Option<DateTime<Utc>>,
    pub(crate) clock: Clock,
}

enum Readback {
    Projection,
    CallbackReceipt(String),
    CompactedProjection,
}

struct Decision<T> {
    value: T,
    events: Vec<OrchestrationEvent>,
    changed: bool,
    valid_before: Option<DateTime<Utc>>,
    readback: Readback,
}

impl<T> Decision<T> {
    fn unchanged(value: T) -> Self {
        Self {
            value,
            events: Vec::new(),
            changed: false,
            valid_before: None,
            readback: Readback::Projection,
        }
    }

    fn changed(
        value: T,
        events: Vec<OrchestrationEvent>,
        valid_before: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            value,
            events,
            changed: true,
            valid_before,
            readback: Readback::Projection,
        }
    }

    fn map_value<U>(self, map: impl FnOnce(T) -> U) -> Decision<U> {
        Decision {
            value: map(self.value),
            events: self.events,
            changed: self.changed,
            valid_before: self.valid_before,
            readback: self.readback,
        }
    }

    fn with_callback_receipt(mut self, request: String) -> Self {
        self.readback = Readback::CallbackReceipt(request);
        self
    }

    fn with_compacted_projection(mut self) -> Self {
        self.readback = Readback::CompactedProjection;
        self
    }
}

struct SelectedPull<T> {
    value: T,
    manifest: OrchestrationManifest,
    projection: Option<FoldState>,
}

impl<T> SelectedPull<T> {
    fn into_projection(self) -> PullResult<(T, OrchestrationManifest, FoldState)> {
        let projection = self.projection.ok_or(PullError::Retryable(
            "worker-pull selected projection was not loaded",
        ))?;
        Ok((self.value, self.manifest, projection))
    }
}

/// Local Flow authority and host for one tenant/workspace.
#[derive(Clone)]
pub struct WorkerPull {
    storage: ScopedStorage,
    compactor: MicroCompactor,
    config: Arc<WorkerPullConfig>,
    config_id: String,
    clock: Clock,
}

impl WorkerPull {
    /// Creates a host for a fresh scope or reopens the same configured pull authority.
    ///
    /// # Errors
    ///
    /// Rejects invalid permit, credential or callback configuration.
    pub fn new(storage: ScopedStorage, config: WorkerPullConfig) -> PullResult<Self> {
        let uri = config.callback_base_url.parse::<axum::http::Uri>().ok();
        let endpoint = uri.as_ref().is_some_and(|uri| {
            matches!(uri.scheme_str(), Some("http" | "https"))
                && uri.authority().is_some()
                && uri.query().is_none()
        });
        let credentials = config.sessions.values().chain([&config.control_token]);
        let mut unique_tokens = BTreeSet::new();
        if !(1..=MAX_PERMITS).contains(&config.permits)
            || storage.scope().workspace_id().is_none()
            || !endpoint
            || config.sessions.is_empty()
            || config.sessions.keys().any(|session| !valid_id(session))
            || credentials
                .into_iter()
                .any(|token| token.len() < 32 || !valid_id(token) || !unique_tokens.insert(token))
        {
            return Err(PullError::Invalid("invalid worker-pull host configuration"));
        }
        let mut mac = hmac(&config.claim_key);
        mac.update(b"arco/flow/worker-pull/config/v1");
        mac.update(&config.permits.to_be_bytes());
        mac.update(config.callback_base_url.as_bytes());
        Ok(Self {
            compactor: MicroCompactor::new(storage.clone())
                .with_delta_tombstones(DeltaTombstoneMode::Emit),
            storage,
            config: Arc::new(config),
            config_id: hex::encode(mac.finalize().into_bytes()),
            clock: Arc::new(Utc::now),
        })
    }

    fn initial_state(&self) -> PullState {
        PullState {
            config_id: self.config_id.clone(),
            permits: vec![None; self.config.permits],
            last_generation: 0,
            accepted_runs: BTreeMap::new(),
            receipts: BTreeMap::new(),
            events: Vec::new(),
        }
    }

    /// Validates a plan's worker-pull shape before a caller reserves its run key.
    ///
    /// # Errors
    /// Rejects unsupported retry bindings, task definitions and event identities.
    pub fn validate_plan(plan: &AcceptedRunPlan) -> PullResult<()> {
        validate_pull_plan(plan).map(|_| ())
    }

    /// Admits the exact Flow plan before any worker can claim a dispatch.
    ///
    /// # Errors
    ///
    /// Rejects conflicting identities and plans outside the local workspace budget.
    /// Returns retryable errors for uncertain selection or storage failure.
    pub async fn accept_plan(&self, plan: &AcceptedRunPlan) -> PullResult<String> {
        let deadline = Self::invocation_deadline();
        let PullPlan {
            run_id,
            plan_id,
            key,
            task_count,
            retry_policy,
        } = validate_pull_plan(plan)?;
        let hash = Self::before(deadline, plan.persist(&self.storage, run_id, plan_id)).await??;
        Self::before(
            deadline,
            load_accepted_run_plan(&self.storage, run_id, plan_id, &hash),
        )
        .await??;
        let events = plan.publication_events(&hash);
        let selected = self
            .transact(deadline, false, None, |fold, state, _, _| {
                if let Some(existing) = state.accepted_runs.get(run_id) {
                    if existing.plan_id == plan_id && existing.sha256 == hash {
                        return Ok(Decision::unchanged(()));
                    }
                    return Err(PullError::Conflict("run has a different accepted plan"));
                }
                if state.accepted_runs.values().any(|run| run.key == key)
                    || events
                        .iter()
                        .any(|event| fold.idempotency_keys.contains_key(&event.idempotency_key))
                {
                    return Err(PullError::Conflict("run key has a different accepted plan"));
                }
                let admitted: usize = state.accepted_runs.values().map(|run| run.tasks).sum();
                if admitted + task_count > MAX_TASKS {
                    return Err(PullError::Conflict("local workspace task budget exhausted"));
                }
                state.accepted_runs.insert(
                    run_id.to_owned(),
                    AcceptedIdentity {
                        plan_id: plan_id.to_owned(),
                        sha256: hash.clone(),
                        key: key.to_owned(),
                        tasks: task_count,
                        retry_policy: retry_policy.map(str::to_owned),
                    },
                );
                Ok(Decision::changed((), events.clone(), None))
            })
            .await?;
        Ok(selected.manifest.revision_ulid)
    }

    /// Records Flow cancellation in the same order as claims and callbacks.
    ///
    /// # Errors
    ///
    /// Rejects missing runs or runs completed without cancellation, and may require
    /// a retry after an uncertain publication.
    pub async fn cancel_run(&self, request: CancelRunRequest) -> PullResult<CancelRunOutcome> {
        let CancelRunRequest {
            run_id,
            reason,
            requested_by,
        } = request;
        let (disposition, _, projection) = self
            .transact(
                Self::invocation_deadline(),
                true,
                None,
                |fold, state, now, _| {
                    let run = fold.runs.get(&run_id).ok_or(PullError::RunNotFound)?;
                    if run.cancel_requested {
                        return Ok(Decision::unchanged(CancelRunDisposition::AlreadyRequested));
                    }
                    if run.state.is_terminal() {
                        return Err(PullError::Conflict("run is already terminal"));
                    }
                    let cancelled: Vec<_> = state
                        .receipts
                        .iter()
                        .filter(|(_, receipt)| {
                            receipt.phase == Phase::Active
                                && receipt.started_at.is_none()
                                && receipt.dispatch.run_id == run_id
                        })
                        .map(|(request, _)| request.clone())
                        .collect();
                    for request in cancelled {
                        state.release(&request, Phase::Cancelled, now)?;
                    }
                    Ok(Decision::changed(
                        CancelRunDisposition::Requested,
                        vec![self.event(
                            OrchestrationEventData::RunCancelRequested {
                                run_id: run_id.clone(),
                                reason: reason.clone(),
                                requested_by: requested_by.clone(),
                            },
                            now,
                        )],
                        None,
                    )
                    .with_compacted_projection())
                },
            )
            .await?
            .into_projection()?;
        let run = run_detail_from_state(&projection, &run_id)
            .ok_or(PullError::Retryable("selected run is missing"))?;
        Ok(CancelRunOutcome {
            run_id,
            state: run.state,
            disposition,
        })
    }

    /// Claims one task for an already authenticated worker session.
    ///
    /// Accepted request IDs remain bound for the claim lifetime and 24 hours
    /// after release. Busy and empty are observations and may be retried with
    /// the same ID.
    ///
    /// # Errors
    ///
    /// Rejects conflicting request identities. Storage or recovery failures are retryable.
    pub async fn claim(&self, session: &str, request: &str) -> PullResult<ClaimReply> {
        let deadline = Self::invocation_deadline();
        if !self.config.sessions.contains_key(session) {
            return Err(PullError::Unauthorized);
        }
        if !valid_id(request) {
            return Err(PullError::Invalid("invalid pull request ID"));
        }
        let mut recover = true;
        let (busy, selected, fold) = loop {
            let (claim, selected, fold) = self
                .transact(deadline, true, None, |fold, state, now, fence| {
                    if recover {
                        let recovery = self.prepare_advance(fold, state, now)?;
                        if recovery.changed {
                            return Ok(recovery.map_value(|_| None));
                        }
                    }
                    Self::prepare_claim(fold, state, now, fence, session, request)
                        .map(|claim| claim.map_value(Some))
                })
                .await?
                .into_projection()?;
            if let Some(busy) = claim {
                break (busy, selected, fold);
            }
            // The selected recovery used this fence's single publication.
            recover = false;
        };
        let authority = selected.worker_pull.as_ref().ok_or(PullError::Retryable(
            "worker-pull authority did not become visible",
        ))?;
        if let Some(receipt) = authority.receipts.get(request) {
            return Self::before(deadline, self.read_grant(authority, receipt, &fold)).await?;
        }
        Ok(if busy {
            ClaimReply::Busy
        } else {
            ClaimReply::Empty
        })
    }

    fn prepare_claim(
        fold: &FoldState,
        state: &mut PullState,
        now: DateTime<Utc>,
        fence: u64,
        session: &str,
        request: &str,
    ) -> PullResult<Decision<bool>> {
        if let Some(receipt) = state.receipts.get(request) {
            if receipt.session != session {
                return Err(PullError::Conflict(
                    "pull request ID belongs to another session",
                ));
            }
            return Ok(Decision::unchanged(false));
        }
        if state.receipts.len() >= MAX_RECEIPTS {
            return Err(PullError::Retryable("worker-pull request history is full"));
        }
        let Some((permit, slot)) = state
            .permits
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| slot.is_none())
        else {
            if Self::has_due_release(fold, state, now) {
                return Err(PullError::Retryable("execution permit recovery is pending"));
            }
            return Ok(Decision::unchanged(true));
        };
        let assigned: BTreeSet<_> = state
            .receipts
            .values()
            .filter(|receipt| receipt.phase == Phase::Active)
            .map(|receipt| &receipt.dispatch.dispatch_id)
            .collect();
        let candidate = fold
            .dispatch_outbox
            .values()
            .filter(|row| !assigned.contains(&row.dispatch_id) && Self::eligible(fold, row))
            .min_by(|a, b| (&a.created_at, &a.dispatch_id).cmp(&(&b.created_at, &b.dispatch_id)));
        let Some(dispatch) = candidate else {
            if Self::dispatch_due(fold, now).next().is_some() {
                return Err(PullError::Retryable(
                    "worker-pull dispatch recovery is pending",
                ));
            }
            return Ok(Decision::unchanged(false));
        };
        if fence <= state.last_generation {
            return Err(PullError::Retryable("claim generation did not advance"));
        }
        // A prepared proposal can never share a generation with a later winner.
        let generation = fence;
        state.last_generation = generation;
        let start_before = now + START_LEASE;
        for receipt in state.receipts.values_mut() {
            if receipt.phase == Phase::Expired
                && receipt.dispatch.dispatch_id == dispatch.dispatch_id
            {
                receipt.phase = Phase::Superseded;
            }
        }
        *slot = Some(generation);
        state.receipts.insert(
            request.into(),
            Receipt {
                dispatch: dispatch.clone(),
                session: session.into(),
                generation,
                permit,
                granted_at: now,
                start_before,
                deadline: start_before,
                started_at: None,
                completion: None,
                ended_at: None,
                phase: Phase::Active,
            },
        );
        Ok(Decision::changed(false, Vec::new(), Some(start_before)))
    }

    fn eligible(fold: &FoldState, row: &DispatchOutboxRow) -> bool {
        row.status == DispatchStatus::Pending
            && !row.attempt_id.is_empty()
            && fold
                .runs
                .get(&row.run_id)
                .is_some_and(|run| !run.cancel_requested && !run.state.is_terminal())
            && fold
                .tasks
                .get(&(row.run_id.clone(), row.task_key.clone()))
                .is_some_and(|task| {
                    task.state == TaskState::Dispatched
                        && task.attempt == row.attempt
                        && task.attempt_id.as_deref() == Some(&row.attempt_id)
                })
    }

    async fn read_grant(
        &self,
        state: &PullState,
        receipt: &Receipt,
        fold: &FoldState,
    ) -> PullResult<ClaimReply> {
        match receipt.phase {
            Phase::Expired => return Ok(ClaimReply::Expired),
            Phase::Superseded => return Ok(ClaimReply::Superseded),
            Phase::Completed => {
                let completion = receipt.completion.as_ref().ok_or(PullError::Retryable(
                    "selected completion receipt is missing",
                ))?;
                return Ok(ClaimReply::Completed {
                    generation: receipt.generation,
                    final_state: completion.state.clone(),
                    accepted_at: completion.accepted_at,
                });
            }
            Phase::Cancelled => return Ok(ClaimReply::Cancelled),
            Phase::Active => {}
        }
        if receipt.deadline <= (self.clock)() {
            return Ok(ClaimReply::Expired);
        }
        if state.permits.get(receipt.permit) != Some(&Some(receipt.generation)) {
            return Err(PullError::Retryable(
                "selected receipt has no execution permit",
            ));
        }
        let row = &receipt.dispatch;
        let task = receipt
            .task(fold)
            .filter(|task| receipt.matches_task(task))
            .ok_or(PullError::Retryable(
                "selected receipt has no matching task attempt",
            ))?;
        let run = fold
            .runs
            .get(&row.run_id)
            .ok_or(PullError::Retryable("selected run is missing"))?;
        if run.cancel_requested {
            return Ok(ClaimReply::Cancelled);
        }
        let token_expires_at = receipt.granted_at + CALLBACK_TOKEN_TTL;
        let mut envelope = dispatch_envelope_for_attempt(
            DispatchEnvelopeSpec {
                tenant_id: self.storage.tenant_id().into(),
                workspace_id: self.storage.workspace_id().into(),
                run_id: row.run_id.clone(),
                task_key: row.task_key.clone(),
                attempt: row.attempt,
                attempt_id: row.attempt_id.clone(),
                dispatch_id: row.dispatch_id.clone(),
                worker_queue: row.worker_queue.clone(),
                callback_base_url: self.config.callback_base_url.clone(),
                task_token: self.capability(receipt),
                token_expires_at,
            },
            Some(task),
        );
        populate_accepted_payload(&self.storage, run, &mut envelope).await?;
        if receipt.deadline <= (self.clock)() {
            return Ok(ClaimReply::Expired);
        }
        Ok(ClaimReply::Granted {
            grant: Box::new(ClaimGrant {
                generation: receipt.generation,
                permit: receipt.permit,
                start_before: receipt.start_before,
                lease_expires_at: receipt.deadline,
                envelope,
            }),
        })
    }

    fn capability_mac(&self, receipt: &Receipt) -> Hmac<Sha256> {
        let mut mac = hmac(&self.config.claim_key);
        mac.update(b"arco/flow/worker-pull/capability/v1");
        mac.update(&receipt.generation.to_be_bytes());
        mac.update(&receipt.dispatch.attempt.to_be_bytes());
        for value in [
            self.storage.tenant_id(),
            self.storage.workspace_id(),
            &receipt.session,
            &receipt.dispatch.dispatch_id,
            &receipt.dispatch.run_id,
            &receipt.dispatch.task_key,
            &receipt.dispatch.attempt_id,
        ] {
            mac.update(&value.len().to_be_bytes());
            mac.update(value.as_bytes());
        }
        mac
    }

    fn capability(&self, receipt: &Receipt) -> String {
        format!(
            "{}.{}",
            receipt.generation,
            hex::encode(self.capability_mac(receipt).finalize().into_bytes())
        )
    }

    fn invocation_deadline() -> TokioInstant {
        TokioInstant::now() + OPERATION_TIMEOUT
    }

    async fn before<T>(deadline: TokioInstant, future: impl Future<Output = T>) -> PullResult<T> {
        if TokioInstant::now() >= deadline {
            return Err(PullError::Retryable("worker-pull invocation timed out"));
        }
        tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| PullError::Retryable("worker-pull invocation timed out"))
    }

    fn event(&self, data: OrchestrationEventData, now: DateTime<Utc>) -> OrchestrationEvent {
        OrchestrationEvent::new_with_timestamp(
            self.storage.tenant_id(),
            self.storage.workspace_id(),
            data,
            now,
        )
    }

    async fn transact<T>(
        &self,
        deadline: TokioInstant,
        sequence: bool,
        mut preloaded: Option<(OrchestrationManifest, FoldState)>,
        mut decide: impl FnMut(
            &FoldState,
            &mut PullState,
            DateTime<Utc>,
            u64,
        ) -> PullResult<Decision<T>>,
    ) -> PullResult<SelectedPull<T>> {
        let lock = DistributedLock::new(
            self.storage.backend().clone(),
            orchestration_compaction_lock_path(),
        );
        let work_deadline = deadline - LOCK_RELEASE_RESERVE;
        for _ in 0..3 {
            let guard = Self::before(work_deadline, lock.acquire(DEFAULT_LOCK_TTL, 8)).await??;
            let result = Self::before(
                work_deadline,
                Box::pin(self.transact_locked(
                    sequence,
                    guard.fencing_token().sequence(),
                    preloaded.take(),
                    &mut decide,
                )),
            )
            .await;
            // Keep the guard outside the canceled work so one-shot hosts can await release.
            Self::before(deadline, guard.release()).await??;
            let result = result?;
            if matches!(&result, Err(PullError::Authority(Error::Core(arco_core::Error::PreconditionFailed { message }))) if message == PULL_RECHECK)
            {
                continue;
            }
            return result;
        }
        Err(PullError::Retryable(
            "worker-pull publication kept changing",
        ))
    }

    fn validate_authority(&self, fold: &FoldState, authority: &PullState) -> PullResult<()> {
        if authority.config_id != self.config_id || authority.permits.len() != self.config.permits {
            return Err(PullError::Conflict(
                "worker-pull host configuration changed",
            ));
        }
        if fold.tasks.len() > MAX_TASKS
            || authority.receipts.len() > MAX_RECEIPTS
            || authority.accepted_runs.len() > MAX_TASKS
            || fold.dispatch_outbox.len() > MAX_TASKS
        {
            return Err(PullError::Retryable(
                "worker-pull authority exceeded its scan budget",
            ));
        }
        authority.validate(fold)
    }

    async fn load_locked_projection(
        &self,
        preloaded: Option<(OrchestrationManifest, FoldState)>,
    ) -> crate::error::Result<(OrchestrationManifest, FoldState)> {
        if let Some((prior, fold)) = preloaded
            && let Some(current) = self.compactor.selected_worker_pull_manifest().await?
        {
            let fold = if current.manifest_id == prior.manifest_id
                && current.revision_ulid == prior.revision_ulid
                && current.shares_projection(&prior)
            {
                fold
            } else {
                self.compactor.load_manifest_projection(&current).await?
            };
            Ok((current, fold))
        } else {
            self.compactor.load_state().await
        }
    }

    async fn transact_locked<T>(
        &self,
        sequence: bool,
        fence: u64,
        preloaded: Option<(OrchestrationManifest, FoldState)>,
        decide: &mut impl FnMut(
            &FoldState,
            &mut PullState,
            DateTime<Utc>,
            u64,
        ) -> PullResult<Decision<T>>,
    ) -> PullResult<SelectedPull<T>> {
        let (manifest, fold) = self.load_locked_projection(preloaded).await?;
        let mut authority = manifest
            .worker_pull
            .clone()
            .unwrap_or_else(|| self.initial_state());
        self.validate_authority(&fold, &authority)?;
        let now = (self.clock)();
        let mut decision = decide(&fold, &mut authority, now, fence)?;
        if !decision.changed && manifest.uses_worker_pull() {
            return Ok(SelectedPull {
                value: decision.value,
                manifest,
                projection: Some(fold),
            });
        }
        if sequence {
            sequence_events(&mut decision.events, &manifest, now)?;
        }
        let preloaded_projection = manifest.uses_worker_pull().then_some((&manifest, &fold));
        let reuse_after_publish = decision.events.is_empty() && preloaded_projection.is_some();
        let update = PullUpdate {
            state: authority,
            events: decision.events,
            expected_revision: manifest
                .uses_worker_pull()
                .then(|| manifest.revision_ulid.clone()),
            preloaded_projection,
            valid_before: decision.valid_before,
            clock: self.clock.clone(),
        };
        let (published, compacted_projection) = self
            .compactor
            .compact_worker_pull(
                &update,
                fence,
                matches!(decision.readback, Readback::CompactedProjection),
            )
            .await?;
        let selected = self
            .compactor
            .read_worker_pull_manifest(&published)
            .await?
            .ok_or(PullError::Retryable(
                "worker-pull revision needs visible readback",
            ))?;
        let projection = match decision.readback {
            Readback::CallbackReceipt(request) => {
                let expected = update
                    .state
                    .receipts
                    .get(&request)
                    .ok_or(PullError::Retryable(
                        "worker-pull callback decision has no receipt",
                    ))?;
                let actual = selected
                    .worker_pull
                    .as_ref()
                    .and_then(|authority| authority.receipts.get(&request));
                if actual != Some(expected) {
                    return Err(PullError::Retryable(
                        "worker-pull selected callback receipt differs from the decision",
                    ));
                }
                None
            }
            Readback::CompactedProjection => {
                let (_, fold) = compacted_projection
                    .filter(|(proposal, _)| {
                        selected.epoch == proposal.epoch && selected.shares_projection(proposal)
                    })
                    .ok_or(PullError::Retryable(
                        "worker-pull selected projection differs from the proposal",
                    ))?;
                Some(fold)
            }
            Readback::Projection
                if reuse_after_publish && selected.shares_projection(&manifest) =>
            {
                Some(fold)
            }
            Readback::Projection => Some(self.compactor.load_manifest_projection(&selected).await?),
        };
        Ok(SelectedPull {
            value: decision.value,
            manifest: selected,
            projection,
        })
    }
}

struct PullPlan<'a> {
    run_id: &'a str,
    plan_id: &'a str,
    key: &'a str,
    task_count: usize,
    retry_policy: Option<&'static str>,
}

fn validate_pull_plan(plan: &AcceptedRunPlan) -> PullResult<PullPlan<'_>> {
    let [
        run_event @ OrchestrationEvent {
            data:
                OrchestrationEventData::RunTriggered {
                    run_id,
                    plan_id,
                    labels,
                    ..
                },
            ..
        },
        plan_event @ OrchestrationEvent {
            data: OrchestrationEventData::PlanCreated { tasks, .. },
            ..
        },
    ] = plan.events.as_slice()
    else {
        return Err(PullError::Invalid(
            "expected a frozen Flow run and task plan",
        ));
    };
    let retry_policy = pull_retry_policy(plan.version, labels)?;
    if !valid_id(run_id)
        || tasks.is_empty()
        || tasks.len() > MAX_TASKS
        || tasks
            .iter()
            .any(|task| task.requires_visible_output || task.heartbeat_timeout_sec == 0)
        || run_event.event_id >= plan_event.event_id
        || plan.events.iter().any(|event| {
            event.event_id.parse::<Ulid>().is_err()
                || event.idempotency_key != event.data.idempotency_key()
        })
    {
        return Err(PullError::Invalid(
            "invalid local Flow plan or native output requirement",
        ));
    }
    let mut keys = BTreeSet::new();
    for task in tasks {
        if task.key.is_empty()
            || task.key.len() > 256
            || task.key.chars().any(char::is_control)
            || !keys.insert(task.key.as_str())
        {
            return Err(PullError::Invalid("invalid or duplicate task key"));
        }
    }
    Ok(PullPlan {
        run_id,
        plan_id,
        key: &run_event.idempotency_key,
        task_count: tasks.len(),
        retry_policy,
    })
}

fn pull_retry_policy(
    version: u32,
    labels: &std::collections::HashMap<String, String>,
) -> PullResult<Option<&'static str>> {
    if labels.contains_key(ACCEPTED_PLAN_SHA256_LABEL) {
        return Err(PullError::Invalid(
            "worker-pull plan supplied its own checksum",
        ));
    }
    match (version, labels.get(RETRY_POLICY_LABEL).map(String::as_str)) {
        (1, None) => Ok(None),
        (2, Some(RIVERS_RETRY_PROFILE)) => Ok(Some(RIVERS_RETRY_PROFILE)),
        _ => Err(PullError::Invalid("unsupported worker-pull retry policy")),
    }
}

fn sequence_events(
    events: &mut [OrchestrationEvent],
    manifest: &OrchestrationManifest,
    now: DateTime<Utc>,
) -> PullResult<()> {
    let millis = u64::try_from(now.timestamp_millis())
        .map_err(|_| PullError::Retryable("invalid server time"))?;
    let mut last = manifest
        .watermarks
        .last_committed_event_id
        .as_ref()
        .map_or_else(|| Ok(Ulid::nil()), |value| value.parse::<Ulid>())
        .map_err(|_| PullError::Retryable("invalid selected event ID"))?;
    for event in events {
        last = if last.timestamp_ms() < millis {
            Ulid::from_parts(millis, 0)
        } else {
            last.increment()
                .ok_or(PullError::Retryable("selected event ID exhausted"))?
        };
        event.event_id = last.to_string();
    }
    Ok(())
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[allow(clippy::expect_used)]
fn hmac(key: &[u8; 32]) -> Hmac<Sha256> {
    Hmac::new_from_slice(key).expect("HMAC accepts a 32-byte key")
}
