use super::{
    AdvanceReply, DEDUP_HORIZON, Decision, DispatchOutboxRow, FoldState, LOCK_RELEASE_RESERVE,
    MAX_ADVANCE_ITEMS, Phase, PullError, PullResult, PullState, Receipt, TaskState, WorkerPull,
};
use crate::orchestration::{
    controllers::{anti_entropy::AntiEntropySweeper, ready_dispatch::ReadyDispatchController},
    events::{OrchestrationEventData, TaskOutcome},
};
use chrono::{DateTime, Utc};

impl WorkerPull {
    /// Advances one bounded batch in the configured workspace independently of worker traffic.
    ///
    /// # Errors
    ///
    /// Returns an error on an uncertain read or publication. Callers may retry.
    pub async fn advance(&self) -> PullResult<AdvanceReply> {
        let deadline = Self::invocation_deadline();
        let preloaded = Self::before(deadline - LOCK_RELEASE_RESERVE, async {
            let Some(manifest) = self.compactor.selected_worker_pull_manifest().await? else {
                return Ok::<_, crate::error::Error>(None);
            };
            let fold = self.compactor.load_manifest_projection(&manifest).await?;
            Ok(Some((manifest, fold)))
        })
        .await??;

        // An idle hint describes the immutable head observed by this invocation.
        if let Some((manifest, fold)) = &preloaded
            && let Some(state) = &manifest.worker_pull
        {
            self.validate_authority(fold, state)?;
            let now = (self.clock)();
            if !Self::advance_due(fold, state, now) {
                return Ok(Self::advance_reply(
                    0,
                    manifest.revision_ulid.clone(),
                    fold,
                    state,
                    now,
                ));
            }
        }

        let (advanced, selected, fold) = self
            .transact(deadline, true, preloaded, |fold, state, now, _| {
                self.prepare_advance(fold, state, now)
                    .map(Decision::with_compacted_projection)
            })
            .await?
            .into_projection()?;

        let state = selected.worker_pull.as_ref().ok_or(PullError::Retryable(
            "worker-pull authority did not become visible",
        ))?;
        Ok(Self::advance_reply(
            advanced,
            selected.revision_ulid,
            &fold,
            state,
            (self.clock)(),
        ))
    }

    fn advance_reply(
        advanced: usize,
        observed_revision: String,
        fold: &FoldState,
        state: &PullState,
        now: DateTime<Utc>,
    ) -> AdvanceReply {
        let active = state
            .receipts
            .values()
            .filter(|receipt| receipt.phase == Phase::Active);
        let next_deadline = active
            .map(|receipt| receipt.deadline)
            .chain(
                fold.tasks
                    .values()
                    .filter(|task| {
                        task.state == TaskState::RetryWait && Self::run_open(fold, &task.run_id)
                    })
                    .filter_map(|task| task.retry_not_before),
            )
            .min();

        AdvanceReply {
            observed_revision,
            advanced,
            immediate_work: Self::advance_due(fold, state, now),
            next_deadline,
        }
    }

    fn advance_due(fold: &FoldState, state: &PullState, now: DateTime<Utc>) -> bool {
        Self::has_due_release(fold, state, now)
            || Self::dispatch_due(fold, now).next().is_some()
            || state
                .receipts
                .values()
                .any(|receipt| Self::receipt_due(receipt, now))
    }

    pub(super) fn prepare_advance(
        &self,
        fold: &FoldState,
        state: &mut PullState,
        now: DateTime<Utc>,
    ) -> PullResult<Decision<usize>> {
        let mut events = Vec::new();
        let mut advanced = 0;
        let mut due = Vec::new();
        for (request, receipt) in &state.receipts {
            if receipt.phase != Phase::Active {
                continue;
            }
            let Some(phase) = Self::due_release(fold, receipt, now) else {
                continue;
            };
            if receipt.started_at.is_some() {
                let task = receipt
                    .task(fold)
                    .filter(|task| receipt.matches_task(task) && task.state == TaskState::Running)
                    .ok_or(PullError::Retryable(
                        "expired running claim has no matching attempt",
                    ))?;
                let outcome = if phase == Phase::Cancelled {
                    TaskOutcome::Cancelled
                } else {
                    TaskOutcome::Failed
                };
                events
                    .push(self.event(Self::finish_fact(fold, task, receipt, outcome, None)?, now));
            }
            due.push((request.clone(), phase));
            advanced += 1;
            if advanced == MAX_ADVANCE_ITEMS {
                break;
            }
        }
        for (request, phase) in due {
            state.release(&request, phase, now)?;
        }

        let mut dispatches: Vec<_> = Self::dispatch_due(fold, now).collect();
        dispatches.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));
        for (_, dispatch) in dispatches.into_iter().take(MAX_ADVANCE_ITEMS - advanced) {
            events.push(self.event(dispatch, now));
            advanced += 1;
        }

        let before = state.receipts.len();
        let mut prune = MAX_ADVANCE_ITEMS - advanced;
        state.receipts.retain(|_, receipt| {
            if prune > 0 && Self::receipt_due(receipt, now) {
                prune -= 1;
                false
            } else {
                true
            }
        });
        if advanced == 0 && before == state.receipts.len() {
            Ok(Decision::unchanged(0))
        } else {
            Ok(Decision::changed(advanced, events, None))
        }
    }

    fn receipt_due(receipt: &Receipt, now: DateTime<Utc>) -> bool {
        receipt
            .ended_at
            .is_some_and(|ended| ended + DEDUP_HORIZON <= now)
    }

    pub(super) fn has_due_release(fold: &FoldState, state: &PullState, now: DateTime<Utc>) -> bool {
        state.receipts.values().any(|receipt| {
            receipt.phase == Phase::Active && Self::due_release(fold, receipt, now).is_some()
        })
    }

    fn due_release(fold: &FoldState, receipt: &Receipt, now: DateTime<Utc>) -> Option<Phase> {
        if !Self::run_open(fold, &receipt.dispatch.run_id)
            && (receipt.started_at.is_none() || receipt.deadline <= now)
        {
            Some(Phase::Cancelled)
        } else if receipt.deadline <= now {
            Some(Phase::Expired)
        } else {
            None
        }
    }

    fn run_open(fold: &FoldState, run_id: &str) -> bool {
        fold.runs
            .get(run_id)
            .is_some_and(|run| !run.cancel_requested && !run.state.is_terminal())
    }

    pub(super) fn dispatch_due(
        fold: &FoldState,
        now: DateTime<Utc>,
    ) -> impl Iterator<Item = ((Option<DateTime<Utc>>, &str, &str), OrchestrationEventData)> {
        let controller = ReadyDispatchController::with_defaults();
        fold.tasks.values().filter_map(move |task| {
            if !Self::run_open(fold, &task.run_id) {
                return None;
            }
            let action = match task.state {
                TaskState::Ready => controller.reconcile_task(task, fold)?,
                TaskState::RetryWait => {
                    let attempt = AntiEntropySweeper::due_retry_attempt(task, now)?;
                    let dispatch_id =
                        DispatchOutboxRow::dispatch_id(&task.run_id, &task.task_key, attempt);
                    if fold.dispatch_outbox.contains_key(&dispatch_id) {
                        return None;
                    }
                    controller.dispatch_action(task, attempt, dispatch_id)
                }
                _ => return None,
            };
            Some((
                (
                    task.retry_not_before.or(task.ready_at),
                    task.run_id.as_str(),
                    task.task_key.as_str(),
                ),
                action.into_event_data()?,
            ))
        })
    }
}
