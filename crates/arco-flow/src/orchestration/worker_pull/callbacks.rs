use super::{
    CALLBACK_TOKEN_TTL, Completion, Decision, FoldState, Phase, PullError, PullResult, PullState,
    Receipt, TaskRow, TaskState, WorkerPull,
};
use crate::orchestration::events::{OrchestrationEventData, TaskOutcome};
use arco_worker_contract::{
    HeartbeatRequest, HeartbeatResponse, TaskCompletedRequest, TaskCompletedResponse,
    TaskStartedRequest, TaskStartedResponse, WorkerOutcome, callback_task_id,
};
use chrono::{DateTime, TimeDelta, Utc};
use hmac::Mac;
use sha2::{Digest, Sha256};

struct Callback<'a> {
    task_id: &'a str,
    session: &'a str,
    attempt: u32,
    attempt_id: &'a str,
    token: &'a str,
}

impl WorkerPull {
    pub(super) async fn started(
        &self,
        task_id: &str,
        token: &str,
        request: &TaskStartedRequest,
    ) -> PullResult<TaskStartedResponse> {
        let deadline = Self::invocation_deadline();
        let callback = Callback {
            task_id,
            token,
            session: &request.worker_id,
            attempt: request.attempt,
            attempt_id: &request.attempt_id,
        };
        let server_time = self
            .transact(deadline, true, None, |fold, state, now, _| {
                let (id, receipt) = self.authenticate(state, &callback, now)?;
                let task = Self::current_task(fold, state, receipt, now)?;
                if fold
                    .runs
                    .get(&task.run_id)
                    .is_some_and(|run| run.cancel_requested)
                {
                    return Err(PullError::Conflict("run cancellation precedes task start"));
                }
                if let Some(server_time) = receipt.started_at {
                    return Ok(Decision::unchanged(server_time));
                }
                let deadline = receipt.deadline;
                let running_deadline = Self::renewed_deadline(receipt, task, now);
                let event = self.event(
                    OrchestrationEventData::TaskStarted {
                        run_id: task.run_id.clone(),
                        task_key: task.task_key.clone(),
                        attempt: task.attempt,
                        attempt_id: receipt.dispatch.attempt_id.clone(),
                        worker_id: receipt.session.clone(),
                    },
                    now,
                );
                let receipt = state.receipts.get_mut(&id).ok_or(PullError::Retryable(
                    "authenticated claim receipt is missing",
                ))?;
                receipt.started_at = Some(now);
                receipt.deadline = running_deadline;
                Ok(
                    Decision::changed(now, vec![event], Some(deadline.min(running_deadline)))
                        .with_callback_receipt(id),
                )
            })
            .await?
            .value;
        Ok(TaskStartedResponse {
            acknowledged: true,
            server_time,
        })
    }

    pub(super) async fn heartbeat(
        &self,
        task_id: &str,
        token: &str,
        request: &HeartbeatRequest,
    ) -> PullResult<HeartbeatResponse> {
        let deadline = Self::invocation_deadline();
        if request.progress_pct.is_some_and(|progress| progress > 100) {
            return Err(PullError::Invalid("progress percentage exceeds 100"));
        }
        let callback = Callback {
            task_id,
            token,
            session: &request.worker_id,
            attempt: request.attempt,
            attempt_id: &request.attempt_id,
        };
        let reply = self
            .transact(deadline, true, None, |fold, state, now, _| {
                let (id, receipt) = self.authenticate(state, &callback, now)?;
                let task = Self::current_task(fold, state, receipt, now)?;
                if receipt.started_at.is_none() {
                    return Err(PullError::Conflict("task has not started"));
                }
                let cancelled = fold
                    .runs
                    .get(&task.run_id)
                    .is_some_and(|run| run.cancel_requested);
                let reply = HeartbeatResponse {
                    acknowledged: true,
                    should_cancel: cancelled,
                    cancel_reason: cancelled.then(|| "run cancellation requested".to_string()),
                    server_time: now,
                };
                if cancelled {
                    return Ok(Decision::unchanged(reply));
                }
                let old_deadline = receipt.deadline;
                let renewed = old_deadline.max(Self::renewed_deadline(receipt, task, now));
                let event = self.event(
                    OrchestrationEventData::TaskHeartbeat {
                        run_id: task.run_id.clone(),
                        task_key: task.task_key.clone(),
                        attempt: task.attempt,
                        attempt_id: receipt.dispatch.attempt_id.clone(),
                        worker_id: receipt.session.clone(),
                        heartbeat_at: Some(now),
                        progress_pct: request.progress_pct,
                        message: request.message.clone(),
                    },
                    now,
                );
                if task
                    .last_heartbeat_at
                    .is_some_and(|previous| previous.timestamp_millis() >= now.timestamp_millis())
                {
                    return Ok(Decision::unchanged(reply));
                }
                state
                    .receipts
                    .get_mut(&id)
                    .ok_or(PullError::Retryable(
                        "authenticated claim receipt is missing",
                    ))?
                    .deadline = renewed;
                Ok(Decision::changed(reply, vec![event], Some(old_deadline))
                    .with_callback_receipt(id))
            })
            .await?
            .value;
        Ok(reply)
    }

    pub(super) async fn completed(
        &self,
        task_id: &str,
        token: &str,
        request: &TaskCompletedRequest,
    ) -> PullResult<TaskCompletedResponse> {
        let deadline = Self::invocation_deadline();
        if request
            .output
            .as_ref()
            .is_some_and(|output| output.publication.is_some())
        {
            return Err(PullError::Invalid(
                "local worker pull cannot verify native publication",
            ));
        }
        let bytes = arco_core::canonical_json::to_canonical_bytes(request)
            .map_err(|error| crate::error::Error::serialization(error.to_string()))?;
        let digest = hex::encode(Sha256::digest(bytes));
        let callback = Callback {
            task_id,
            token,
            session: &request.worker_id,
            attempt: request.attempt,
            attempt_id: &request.attempt_id,
        };
        let completion = self
            .transact(deadline, true, None, |fold, state, now, _| {
                let (id, receipt) = self.authenticate(state, &callback, now)?;
                if let Some(completion) = &receipt.completion {
                    if completion.digest != digest {
                        return Err(PullError::Conflict(
                            "completion differs from the accepted callback",
                        ));
                    }
                    return Ok(Decision::unchanged(completion.clone()));
                }
                let task = Self::current_task(fold, state, receipt, now)?;
                if receipt.started_at.is_none() {
                    return Err(PullError::Conflict("task has not started"));
                }
                let outcome = if fold
                    .runs
                    .get(&task.run_id)
                    .is_some_and(|run| run.cancel_requested)
                {
                    TaskOutcome::Cancelled
                } else {
                    match request.outcome {
                        WorkerOutcome::Succeeded => TaskOutcome::Succeeded,
                        WorkerOutcome::Failed => TaskOutcome::Failed,
                        WorkerOutcome::Cancelled => TaskOutcome::Cancelled,
                    }
                };
                let deadline = receipt.deadline;
                let event = self.event(
                    Self::finish_fact(fold, task, receipt, outcome, Some(request))?,
                    now,
                );
                let state_name = match outcome {
                    TaskOutcome::Succeeded => "SUCCEEDED",
                    TaskOutcome::Failed => "FAILED",
                    TaskOutcome::Cancelled => "CANCELLED",
                    TaskOutcome::Skipped => "SKIPPED",
                };
                let completion = Completion {
                    digest: digest.clone(),
                    state: state_name.into(),
                    accepted_at: now,
                };
                state.release(&id, Phase::Completed, now)?;
                state
                    .receipts
                    .get_mut(&id)
                    .ok_or(PullError::Retryable(
                        "authenticated claim receipt is missing",
                    ))?
                    .completion = Some(completion.clone());
                Ok(Decision::changed(completion, vec![event], Some(deadline))
                    .with_callback_receipt(id))
            })
            .await?
            .value;
        Ok(TaskCompletedResponse {
            acknowledged: true,
            final_state: completion.state,
            server_time: completion.accepted_at,
        })
    }

    fn authenticate<'a>(
        &self,
        state: &'a PullState,
        callback: &Callback<'_>,
        now: DateTime<Utc>,
    ) -> PullResult<(String, &'a Receipt)> {
        let (generation, signature) = callback
            .token
            .split_once('.')
            .ok_or(PullError::Unauthorized)?;
        let generation = generation
            .parse::<u64>()
            .map_err(|_| PullError::Unauthorized)?;
        let mut bytes = [0; 32];
        hex::decode_to_slice(signature, &mut bytes).map_err(|_| PullError::Unauthorized)?;
        let (request, receipt) = state
            .receipts
            .iter()
            .find(|(_, receipt)| receipt.generation == generation)
            .ok_or(PullError::Unauthorized)?;
        self.capability_mac(receipt)
            .verify_slice(&bytes)
            .map_err(|_| PullError::Unauthorized)?;
        let row = &receipt.dispatch;
        if callback.task_id != callback_task_id(&row.run_id, &row.task_key)
            || callback.session != receipt.session
            || callback.attempt != row.attempt
            || callback.attempt_id != row.attempt_id
        {
            return Err(PullError::Unauthorized);
        }
        if receipt.granted_at + CALLBACK_TOKEN_TTL <= now {
            return Err(PullError::Conflict("claim capability expired"));
        }
        Ok((request.clone(), receipt))
    }

    fn current_task<'a>(
        fold: &'a FoldState,
        state: &PullState,
        receipt: &Receipt,
        now: DateTime<Utc>,
    ) -> PullResult<&'a TaskRow> {
        if receipt.phase != Phase::Active
            || receipt.deadline <= now
            || state.permits.get(receipt.permit) != Some(&Some(receipt.generation))
        {
            return Err(PullError::Conflict(
                "claim no longer owns its execution permit",
            ));
        }
        let expected_state = if receipt.started_at.is_some() {
            TaskState::Running
        } else {
            TaskState::Dispatched
        };
        receipt
            .task(fold)
            .filter(|task| receipt.matches_task(task) && task.state == expected_state)
            .ok_or(PullError::Conflict(
                "claim no longer owns the current attempt",
            ))
    }

    fn renewed_deadline(receipt: &Receipt, task: &TaskRow, now: DateTime<Utc>) -> DateTime<Utc> {
        (now + TimeDelta::seconds(i64::from(task.heartbeat_timeout_sec)))
            .min(receipt.granted_at + CALLBACK_TOKEN_TTL)
    }

    pub(super) fn finish_fact(
        fold: &FoldState,
        task: &TaskRow,
        receipt: &Receipt,
        outcome: TaskOutcome,
        request: Option<&TaskCompletedRequest>,
    ) -> PullResult<OrchestrationEventData> {
        let mut output = request
            .and_then(|r| r.output.clone())
            .filter(|_| outcome == TaskOutcome::Succeeded);
        if let Some(output) = &mut output {
            output.output_visibility_state = None;
            output.published_at = None;
            output.publish_error = None;
        }
        let error = request.and_then(|r| r.error.clone()).map(|mut error| {
            error.retryable = Some(error.effective_retryable());
            error
        });
        let partial_progress_json = request
            .and_then(|r| r.partial_progress.as_ref())
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| crate::error::Error::serialization(error.to_string()))?;
        Ok(OrchestrationEventData::TaskCompletionRecorded {
            run_id: task.run_id.clone(),
            task_key: task.task_key.clone(),
            attempt: task.attempt,
            attempt_id: receipt.dispatch.attempt_id.clone(),
            worker_id: receipt.session.clone(),
            outcome,
            materialization_id: output
                .as_ref()
                .and_then(|output| output.materialization_id.clone()),
            error_message: error
                .as_ref()
                .map(|error| error.message.clone())
                .or_else(|| {
                    request
                        .is_none()
                        .then(|| "execution lease expired".to_string())
                }),
            output,
            error,
            metrics: request.and_then(|r| r.metrics.clone()),
            cancelled_during_phase: request.and_then(|r| r.cancelled_during_phase.clone()),
            partial_progress_json,
            asset_key: task.asset_key.clone(),
            partition_key: task.partition_key.clone(),
            code_version: fold
                .runs
                .get(&task.run_id)
                .and_then(|run| run.code_version.clone()),
            output_visibility: None,
        })
    }
}
