//! Flow-owned runtime mapping for the authoritative `arco.orchestration.v1` contract.

#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::missing_errors_doc,
    clippy::option_if_let_else,
    clippy::too_many_lines,
    clippy::unnecessary_lazy_evaluations,
    clippy::unnecessary_wraps
)]

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use thiserror::Error;

use arco_core::partition::{
    PartitionKey as CorePartitionKey, PartitionKeyParseError, ScalarValue as CoreScalarValue,
};
use arco_proto::arco::common::v1 as common_proto;
use arco_proto::arco::common::v1::scalar_value;
use arco_proto::arco::orchestration::v1 as proto;
use arco_proto::arco::orchestration::v1::orchestration_event_envelope;
use arco_proto::arco::orchestration::v1::trigger_info;

use crate::orchestration::callbacks::{
    ErrorCategory as CallbackErrorCategory, TaskError as CallbackTaskError,
    TaskMetrics as CallbackTaskMetrics, TaskOutput as CallbackTaskOutput,
};
use crate::orchestration::compactor::fold::{
    RunRow, RunState as FoldRunState, TaskRow, TaskState as FoldTaskState,
};
use crate::orchestration::events::{
    BackfillState, ChunkState, OrchestrationEvent, OrchestrationEventData, OutputVisibilityState,
    PartitionSelector, RunRequest, SensorEvalStatus, SourceRef, TaskDef, TaskOutcome, TickStatus,
    TimerType, TriggerInfo, TriggerSource,
};
use crate::run::{Run, RunState as LegacyRunState, TriggerType};
use crate::task::{
    TaskError as LegacyTaskError, TaskErrorCategory as LegacyTaskErrorCategory,
    TaskExecution as LegacyTaskExecution, TaskOutput as LegacyTaskOutput,
    TaskState as LegacyTaskState,
};
use crate::task_key::{TaskKey as LegacyTaskKey, TaskOperation as LegacyTaskOperation};

/// Errors produced while converting between runtime orchestration types and proto types.
#[derive(Debug, Error)]
pub enum OrchestrationProtoError {
    /// A required proto timestamp is missing.
    #[error("{field} timestamp is required")]
    MissingTimestamp {
        /// Fully-qualified field name for the missing timestamp.
        field: &'static str,
    },
    /// A proto timestamp could not be converted to chrono.
    #[error("{field} timestamp is invalid")]
    InvalidTimestamp {
        /// Fully-qualified field name for the invalid timestamp.
        field: &'static str,
    },
    /// A required event payload was not present.
    #[error("orchestration event payload is required")]
    MissingEventKind,
    /// An unknown enum value was supplied.
    #[error("unknown {enum_name} value: {value}")]
    UnknownEnum {
        /// Enum name for context.
        enum_name: &'static str,
        /// Raw value received from the proto payload.
        value: i32,
    },
    /// A runtime canonical partition key could not be parsed.
    #[error("invalid canonical partition key '{value}': {source}")]
    InvalidPartitionKey {
        /// Raw canonical string.
        value: String,
        /// Underlying parse error.
        source: PartitionKeyParseError,
    },
    /// The proto trigger kind is unsupported for the runtime target.
    #[error("{context} trigger kind '{kind}' is not supported by the runtime mapping")]
    UnsupportedTriggerKind {
        /// Mapping context.
        context: &'static str,
        /// Trigger kind name.
        kind: &'static str,
    },
    /// A runtime event kind cannot be represented in the proto contract.
    #[error("orchestration event kind '{kind}' is not supported by the proto mapping")]
    UnsupportedEventKind {
        /// Event kind name.
        kind: &'static str,
    },
}

/// Converts a runtime orchestration event into the authoritative proto envelope.
///
/// # Errors
///
/// Returns an error if a runtime field cannot be represented by the proto contract.
pub fn event_to_proto_envelope(
    event: &OrchestrationEvent,
) -> Result<proto::OrchestrationEventEnvelope, OrchestrationProtoError> {
    Ok(proto::OrchestrationEventEnvelope {
        event_id: event.event_id.clone(),
        event_version: event.event_version,
        timestamp: Some(chrono_to_protobuf(event.timestamp)),
        source: event.source.clone(),
        idempotency_key: event.idempotency_key.clone(),
        correlation_id: event.correlation_id.clone(),
        causation_id: event.causation_id.clone(),
        event: Some(event_data_to_proto(&event.data)?),
    })
}

/// Converts a proto orchestration envelope into the runtime representation.
///
/// `tenant_id` and `workspace_id` are carried outside the proto envelope and must
/// be supplied by the caller from request/runtime context.
///
/// # Errors
///
/// Returns an error if the proto payload is invalid or cannot be represented by
/// the runtime event types.
pub fn event_from_proto_envelope(
    tenant_id: &str,
    workspace_id: &str,
    envelope: &proto::OrchestrationEventEnvelope,
) -> Result<OrchestrationEvent, OrchestrationProtoError> {
    let data = proto_event_to_runtime(
        envelope
            .event
            .as_ref()
            .ok_or(OrchestrationProtoError::MissingEventKind)?,
    )?;

    Ok(OrchestrationEvent {
        event_id: envelope.event_id.clone(),
        event_type: data.event_type().to_string(),
        event_version: envelope.event_version,
        timestamp: protobuf_to_chrono(
            envelope.timestamp.as_ref(),
            "orchestration_event_envelope.timestamp",
        )?,
        source: envelope.source.clone(),
        tenant_id: tenant_id.to_string(),
        workspace_id: workspace_id.to_string(),
        idempotency_key: envelope.idempotency_key.clone(),
        correlation_id: envelope
            .correlation_id
            .clone()
            .or_else(|| data.run_id().map(ToString::to_string)),
        causation_id: envelope.causation_id.clone(),
        data,
    })
}

/// Maps the event-driven fold run row to the public proto state.
#[must_use]
pub fn fold_run_state_to_proto(run: &RunRow) -> proto::RunState {
    if run.cancel_requested && !run.state.is_terminal() {
        return proto::RunState::Cancelling;
    }

    match run.state {
        FoldRunState::Triggered => proto::RunState::Pending,
        FoldRunState::Running => proto::RunState::Running,
        FoldRunState::Succeeded => proto::RunState::Succeeded,
        FoldRunState::Failed => proto::RunState::Failed,
        FoldRunState::Cancelled => proto::RunState::Cancelled,
    }
}

/// Maps the event-driven fold task row to the public proto state.
#[must_use]
pub fn fold_task_state_to_proto(task: &TaskRow) -> proto::TaskState {
    match task.state {
        FoldTaskState::Planned => proto::TaskState::Planned,
        FoldTaskState::Blocked => proto::TaskState::Pending,
        FoldTaskState::Ready => proto::TaskState::Ready,
        FoldTaskState::Dispatched => proto::TaskState::Dispatched,
        FoldTaskState::Running => proto::TaskState::Running,
        FoldTaskState::RetryWait => proto::TaskState::RetryWait,
        FoldTaskState::Skipped => proto::TaskState::Skipped,
        FoldTaskState::Cancelled => proto::TaskState::Cancelled,
        FoldTaskState::Failed => proto::TaskState::Failed,
        FoldTaskState::Succeeded => proto::TaskState::Succeeded,
    }
}

/// Maps the legacy scheduler run state to the public proto state.
#[must_use]
pub fn legacy_run_state_to_proto(state: LegacyRunState) -> proto::RunState {
    match state {
        LegacyRunState::Pending => proto::RunState::Pending,
        LegacyRunState::Running => proto::RunState::Running,
        LegacyRunState::Succeeded => proto::RunState::Succeeded,
        LegacyRunState::Failed => proto::RunState::Failed,
        LegacyRunState::Cancelling => proto::RunState::Cancelling,
        LegacyRunState::Cancelled => proto::RunState::Cancelled,
        LegacyRunState::TimedOut => proto::RunState::TimedOut,
    }
}

/// Maps the legacy scheduler task state to the public proto state.
#[must_use]
pub fn legacy_task_state_to_proto(state: LegacyTaskState) -> proto::TaskState {
    match state {
        LegacyTaskState::Planned => proto::TaskState::Planned,
        LegacyTaskState::Pending => proto::TaskState::Pending,
        LegacyTaskState::Ready => proto::TaskState::Ready,
        LegacyTaskState::Queued => proto::TaskState::Queued,
        LegacyTaskState::Dispatched => proto::TaskState::Dispatched,
        LegacyTaskState::Running => proto::TaskState::Running,
        LegacyTaskState::Succeeded => proto::TaskState::Succeeded,
        LegacyTaskState::Failed => proto::TaskState::Failed,
        LegacyTaskState::Skipped => proto::TaskState::Skipped,
        LegacyTaskState::Cancelled => proto::TaskState::Cancelled,
        LegacyTaskState::RetryWait => proto::TaskState::RetryWait,
    }
}

/// Builds the public proto task execution from the event-driven fold read-model row.
#[must_use]
pub fn task_row_to_proto_execution(task: &TaskRow) -> proto::TaskExecution {
    proto::TaskExecution {
        task_key: task.task_key.clone(),
        state: fold_task_state_to_proto(task) as i32,
        attempt: task.attempt,
        queued_at: None,
        started_at: task.started_at.map(chrono_to_protobuf),
        completed_at: task.completed_at.map(chrono_to_protobuf),
        worker_id: None,
        output: public_output_from_materialization(task.materialization_id.as_deref()),
        error: task.error_message.as_ref().map(|message| proto::TaskError {
            category: proto::TaskErrorCategory::Unknown as i32,
            message: message.clone(),
            detail: None,
            retryable: None,
        }),
        metrics: None,
        semantic_key: None,
        priority: 0,
    }
}

/// Builds the public proto run from the event-driven fold read-model row.
#[must_use]
pub fn run_row_to_proto_run(run: &RunRow, tasks: &[&TaskRow]) -> proto::Run {
    proto::Run {
        run_id: run.run_id.clone(),
        plan_id: run.plan_id.clone(),
        state: fold_run_state_to_proto(run) as i32,
        created_at: Some(chrono_to_protobuf(run.triggered_at)),
        started_at: None,
        completed_at: run.completed_at.map(chrono_to_protobuf),
        task_executions: tasks
            .iter()
            .map(|task| task_row_to_proto_execution(task))
            .collect(),
        labels: hash_to_btree(&run.labels),
        trigger: None,
    }
}

/// Builds the public proto task execution from the legacy in-memory runtime model.
pub fn legacy_task_execution_to_proto_execution(
    task: &LegacyTaskExecution,
) -> Result<proto::TaskExecution, OrchestrationProtoError> {
    Ok(proto::TaskExecution {
        task_key: task.task_id.to_string(),
        state: legacy_task_state_to_proto(task.state) as i32,
        attempt: task.attempt,
        queued_at: task.queued_at.map(chrono_to_protobuf),
        started_at: task.started_at.map(chrono_to_protobuf),
        completed_at: task.completed_at.map(chrono_to_protobuf),
        worker_id: task.worker_id.clone(),
        output: task
            .output
            .as_ref()
            .map(public_output_from_legacy_task_output),
        error: task.error.as_ref().map(legacy_task_error_to_proto),
        metrics: Some(proto::TaskMetrics {
            cpu_time_ms: (task.metrics.cpu_time_ms > 0).then(|| task.metrics.cpu_time_ms as u64),
            peak_memory_bytes: (task.metrics.peak_memory_bytes > 0)
                .then(|| task.metrics.peak_memory_bytes as u64),
            io_read_bytes: (task.metrics.bytes_read > 0).then(|| task.metrics.bytes_read as u64),
            io_write_bytes: (task.metrics.bytes_written > 0)
                .then(|| task.metrics.bytes_written as u64),
        }),
        semantic_key: task.task_key.as_ref().map(task_key_to_proto).transpose()?,
        priority: task.priority,
    })
}

/// Builds the public proto run from the legacy in-memory runtime model.
pub fn legacy_run_to_proto_run(run: &Run) -> Result<proto::Run, OrchestrationProtoError> {
    Ok(proto::Run {
        run_id: run.id.to_string(),
        plan_id: run.plan_id.clone(),
        state: legacy_run_state_to_proto(run.state) as i32,
        created_at: Some(chrono_to_protobuf(run.created_at)),
        started_at: run.started_at.map(chrono_to_protobuf),
        completed_at: run.completed_at.map(chrono_to_protobuf),
        task_executions: run
            .task_executions
            .iter()
            .map(legacy_task_execution_to_proto_execution)
            .collect::<Result<Vec<_>, _>>()?,
        labels: hash_to_btree(&run.labels),
        trigger: Some(legacy_run_trigger_to_proto(&run.trigger)),
    })
}

fn event_data_to_proto(
    data: &OrchestrationEventData,
) -> Result<orchestration_event_envelope::Event, OrchestrationProtoError> {
    Ok(match data {
        OrchestrationEventData::RunTriggered {
            run_id,
            plan_id,
            trigger,
            root_assets,
            run_key,
            labels,
            code_version,
        } => orchestration_event_envelope::Event::RunTriggered(proto::RunTriggered {
            run_id: run_id.clone(),
            plan_id: plan_id.clone(),
            trigger: Some(trigger_info_to_proto(trigger)?),
            root_assets: root_assets.clone(),
            run_key: run_key.clone(),
            labels: hash_to_btree(labels),
            code_version: code_version.clone(),
        }),
        OrchestrationEventData::PlanCreated {
            run_id,
            plan_id,
            tasks,
        } => orchestration_event_envelope::Event::PlanCreated(proto::PlanCreated {
            run_id: run_id.clone(),
            plan_id: plan_id.clone(),
            tasks: tasks
                .iter()
                .map(task_def_to_proto)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        OrchestrationEventData::RunCancelRequested {
            run_id,
            reason,
            requested_by,
        } => orchestration_event_envelope::Event::RunCancelRequested(proto::RunCancelRequested {
            run_id: run_id.clone(),
            reason: reason.clone(),
            requested_by: requested_by.clone(),
        }),
        OrchestrationEventData::TaskStarted {
            run_id,
            task_key,
            attempt,
            attempt_id,
            worker_id,
        } => orchestration_event_envelope::Event::TaskStarted(proto::TaskStarted {
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            attempt_id: attempt_id.clone(),
            worker_id: worker_id.clone(),
        }),
        OrchestrationEventData::TaskHeartbeat {
            run_id,
            task_key,
            attempt,
            attempt_id,
            worker_id,
            heartbeat_at,
            progress_pct,
            message,
        } => orchestration_event_envelope::Event::TaskHeartbeat(proto::TaskHeartbeat {
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            attempt_id: attempt_id.clone(),
            worker_id: worker_id.clone(),
            heartbeat_at: heartbeat_at.map(chrono_to_protobuf),
            progress_pct: progress_pct.map(u32::from),
            message: message.clone(),
        }),
        OrchestrationEventData::TaskFinished {
            run_id,
            task_key,
            attempt,
            attempt_id,
            worker_id,
            outcome,
            materialization_id,
            error_message,
            output,
            error,
            metrics,
            cancelled_during_phase,
            asset_key,
            partition_key,
            code_version,
            ..
        } => orchestration_event_envelope::Event::TaskFinished(proto::TaskFinished {
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            attempt_id: attempt_id.clone(),
            worker_id: worker_id.clone(),
            outcome: task_outcome_to_proto(*outcome) as i32,
            callback_output: task_finished_output_to_proto(
                materialization_id.as_deref(),
                output.as_ref(),
            ),
            error: task_finished_error_to_proto(error_message.as_deref(), error.as_ref()),
            metrics: metrics.as_ref().map(callback_metrics_to_proto),
            cancelled_during_phase: cancelled_during_phase.clone(),
            asset_key: asset_key.clone(),
            partition_key: string_to_proto_partition_key(partition_key.as_deref())?,
            code_version: code_version.clone(),
        }),
        OrchestrationEventData::TaskOutputVisibilityChanged {
            run_id,
            task_key,
            attempt,
            attempt_id,
            visibility_state,
            published_at,
            publish_error,
        } => orchestration_event_envelope::Event::TaskOutputVisibilityChanged(
            proto::TaskOutputVisibilityChanged {
                run_id: run_id.clone(),
                task_key: task_key.clone(),
                attempt: *attempt,
                attempt_id: attempt_id.clone(),
                visibility_state: output_visibility_state_to_proto(*visibility_state) as i32,
                published_at: published_at.map(chrono_to_protobuf),
                publish_error: publish_error.clone(),
            },
        ),
        OrchestrationEventData::DispatchRequested {
            run_id,
            task_key,
            attempt,
            attempt_id,
            worker_queue,
            dispatch_id,
        } => orchestration_event_envelope::Event::DispatchRequested(proto::DispatchRequested {
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            attempt_id: attempt_id.clone(),
            worker_queue: worker_queue.clone(),
            dispatch_id: dispatch_id.clone(),
        }),
        OrchestrationEventData::TimerRequested {
            timer_id,
            timer_type,
            run_id,
            task_key,
            attempt,
            fire_at,
        } => orchestration_event_envelope::Event::TimerRequested(proto::TimerRequested {
            timer_id: timer_id.clone(),
            timer_type: timer_type_to_proto(*timer_type) as i32,
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            fire_at: Some(chrono_to_protobuf(*fire_at)),
        }),
        OrchestrationEventData::DispatchEnqueued {
            dispatch_id,
            run_id,
            task_key,
            attempt,
            cloud_task_id,
        } => orchestration_event_envelope::Event::DispatchEnqueued(proto::DispatchEnqueued {
            dispatch_id: dispatch_id.clone(),
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            cloud_task_id: cloud_task_id.clone(),
        }),
        OrchestrationEventData::TimerEnqueued {
            timer_id,
            run_id,
            task_key,
            attempt,
            cloud_task_id,
        } => orchestration_event_envelope::Event::TimerEnqueued(proto::TimerEnqueued {
            timer_id: timer_id.clone(),
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
            cloud_task_id: cloud_task_id.clone(),
        }),
        OrchestrationEventData::TimerFired {
            timer_id,
            timer_type,
            run_id,
            task_key,
            attempt,
        } => orchestration_event_envelope::Event::TimerFired(proto::TimerFired {
            timer_id: timer_id.clone(),
            timer_type: timer_type_to_proto(*timer_type) as i32,
            run_id: run_id.clone(),
            task_key: task_key.clone(),
            attempt: *attempt,
        }),
        OrchestrationEventData::ScheduleDefinitionUpserted {
            schedule_id,
            cron_expression,
            timezone,
            catchup_window_minutes,
            asset_selection,
            code_version,
            max_catchup_ticks,
            enabled,
        } => orchestration_event_envelope::Event::ScheduleDefinitionUpserted(
            proto::ScheduleDefinitionUpserted {
                schedule_id: schedule_id.clone(),
                cron_expression: cron_expression.clone(),
                timezone: timezone.clone(),
                catchup_window_minutes: *catchup_window_minutes,
                asset_selection: asset_selection.clone(),
                max_catchup_ticks: *max_catchup_ticks,
                enabled: *enabled,
                code_version: code_version.clone(),
            },
        ),
        OrchestrationEventData::ScheduleTicked {
            schedule_id,
            scheduled_for,
            tick_id,
            definition_version,
            asset_selection,
            partition_selection,
            status,
            run_key,
            request_fingerprint,
        } => {
            let (status_value, skipped_reason, failure_message) = tick_status_to_proto(status);
            orchestration_event_envelope::Event::ScheduleTicked(proto::ScheduleTicked {
                schedule_id: schedule_id.clone(),
                scheduled_for: Some(chrono_to_protobuf(*scheduled_for)),
                tick_id: tick_id.clone(),
                definition_version: definition_version.clone(),
                asset_selection: asset_selection.clone(),
                partition_selection: partition_selection.clone().unwrap_or_default(),
                status: status_value as i32,
                skipped_reason,
                failure_message,
                run_key: run_key.clone(),
                request_fingerprint: request_fingerprint.clone(),
            })
        }
        OrchestrationEventData::SensorEvaluated {
            sensor_id,
            eval_id,
            cursor_before,
            cursor_after,
            expected_state_version,
            trigger_source,
            run_requests,
            status,
        } => {
            let (status_value, error_message) = sensor_eval_status_to_proto(status);
            orchestration_event_envelope::Event::SensorEvaluated(proto::SensorEvaluated {
                sensor_id: sensor_id.clone(),
                eval_id: eval_id.clone(),
                cursor_before: cursor_before.clone(),
                cursor_after: cursor_after.clone(),
                expected_state_version: *expected_state_version,
                trigger_source: Some(trigger_source_to_proto(trigger_source)),
                run_requests: run_requests
                    .iter()
                    .map(run_request_to_proto)
                    .collect::<Vec<_>>(),
                status: status_value as i32,
                error_message,
            })
        }
        OrchestrationEventData::RunRequested {
            run_key,
            request_fingerprint,
            asset_selection,
            partition_selection,
            trigger_source_ref,
            labels,
            code_version,
        } => orchestration_event_envelope::Event::RunRequested(proto::RunRequested {
            run_key: run_key.clone(),
            request_fingerprint: request_fingerprint.clone(),
            asset_selection: asset_selection.clone(),
            partition_selection: partition_selection.clone().unwrap_or_default(),
            trigger: Some(source_ref_to_proto(trigger_source_ref)),
            labels: hash_to_btree(labels),
            code_version: code_version.clone(),
        }),
        OrchestrationEventData::BackfillCreated {
            backfill_id,
            client_request_id,
            asset_selection,
            code_version,
            partition_selector,
            total_partitions,
            chunk_size,
            max_concurrent_runs,
            parent_backfill_id,
        } => orchestration_event_envelope::Event::BackfillCreated(proto::BackfillCreated {
            backfill_id: backfill_id.clone(),
            client_request_id: client_request_id.clone(),
            asset_selection: asset_selection.clone(),
            partition_selector: Some(partition_selector_to_proto(partition_selector)),
            total_partitions: *total_partitions,
            chunk_size: *chunk_size,
            max_concurrent_runs: *max_concurrent_runs,
            parent_backfill_id: parent_backfill_id.clone(),
            code_version: code_version.clone(),
        }),
        OrchestrationEventData::BackfillChunkPlanned {
            backfill_id,
            chunk_id,
            chunk_index,
            partition_keys,
            run_key,
            request_fingerprint,
        } => {
            orchestration_event_envelope::Event::BackfillChunkPlanned(proto::BackfillChunkPlanned {
                backfill_id: backfill_id.clone(),
                chunk_id: chunk_id.clone(),
                chunk_index: *chunk_index,
                partition_keys: partition_keys.clone(),
                run_key: run_key.clone(),
                request_fingerprint: request_fingerprint.clone(),
            })
        }
        OrchestrationEventData::BackfillStateChanged {
            backfill_id,
            from_state,
            to_state,
            state_version,
            changed_by,
        } => {
            orchestration_event_envelope::Event::BackfillStateChanged(proto::BackfillStateChanged {
                backfill_id: backfill_id.clone(),
                from_state: backfill_state_to_proto(*from_state) as i32,
                to_state: backfill_state_to_proto(*to_state) as i32,
                state_version: *state_version,
                changed_by: changed_by.clone(),
            })
        }
        OrchestrationEventData::Unknown => {
            return Err(OrchestrationProtoError::UnsupportedEventKind { kind: "Unknown" });
        }
    })
}

fn proto_event_to_runtime(
    event: &orchestration_event_envelope::Event,
) -> Result<OrchestrationEventData, OrchestrationProtoError> {
    Ok(match event {
        orchestration_event_envelope::Event::RunTriggered(event) => {
            OrchestrationEventData::RunTriggered {
                run_id: event.run_id.clone(),
                plan_id: event.plan_id.clone(),
                trigger: proto_trigger_info_to_runtime(
                    event.trigger.as_ref(),
                    "run_triggered.trigger",
                )?,
                root_assets: event.root_assets.clone(),
                run_key: event.run_key.clone(),
                labels: btree_to_hash(&event.labels),
                code_version: event.code_version.clone(),
            }
        }
        orchestration_event_envelope::Event::PlanCreated(event) => {
            OrchestrationEventData::PlanCreated {
                run_id: event.run_id.clone(),
                plan_id: event.plan_id.clone(),
                tasks: event
                    .tasks
                    .iter()
                    .map(proto_task_def_to_runtime)
                    .collect::<Result<Vec<_>, _>>()?,
            }
        }
        orchestration_event_envelope::Event::RunCancelRequested(event) => {
            OrchestrationEventData::RunCancelRequested {
                run_id: event.run_id.clone(),
                reason: event.reason.clone(),
                requested_by: event.requested_by.clone(),
            }
        }
        orchestration_event_envelope::Event::TaskStarted(event) => {
            OrchestrationEventData::TaskStarted {
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                attempt_id: event.attempt_id.clone(),
                worker_id: event.worker_id.clone(),
            }
        }
        orchestration_event_envelope::Event::TaskHeartbeat(event) => {
            OrchestrationEventData::TaskHeartbeat {
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                attempt_id: event.attempt_id.clone(),
                worker_id: event.worker_id.clone(),
                heartbeat_at: event
                    .heartbeat_at
                    .as_ref()
                    .map(|value| protobuf_to_chrono(Some(value), "task_heartbeat.heartbeat_at"))
                    .transpose()?,
                progress_pct: event.progress_pct.map(|value| value as u8),
                message: event.message.clone(),
            }
        }
        orchestration_event_envelope::Event::TaskFinished(event) => {
            let output = event
                .callback_output
                .as_ref()
                .map(callback_output_from_proto);
            let (error_message, error) = task_finished_error_from_proto(event.error.as_ref())?;
            OrchestrationEventData::TaskFinished {
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                attempt_id: event.attempt_id.clone(),
                worker_id: event.worker_id.clone(),
                outcome: proto_task_outcome_to_runtime(event.outcome)?,
                materialization_id: output
                    .as_ref()
                    .and_then(|value| value.materialization_id.clone()),
                error_message,
                output,
                error,
                metrics: event.metrics.as_ref().map(callback_metrics_from_proto),
                cancelled_during_phase: event.cancelled_during_phase.clone(),
                partial_progress_json: None,
                asset_key: event.asset_key.clone(),
                partition_key: proto_partition_key_to_string(event.partition_key.as_ref())?,
                code_version: event.code_version.clone(),
            }
        }
        orchestration_event_envelope::Event::TaskOutputVisibilityChanged(event) => {
            OrchestrationEventData::TaskOutputVisibilityChanged {
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                attempt_id: event.attempt_id.clone(),
                visibility_state: proto_output_visibility_state_to_runtime(event.visibility_state)?,
                published_at: event
                    .published_at
                    .as_ref()
                    .map(|value| {
                        protobuf_to_chrono(
                            Some(value),
                            "task_output_visibility_changed.published_at",
                        )
                    })
                    .transpose()?,
                publish_error: event.publish_error.clone(),
            }
        }
        orchestration_event_envelope::Event::DispatchRequested(event) => {
            OrchestrationEventData::DispatchRequested {
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                attempt_id: event.attempt_id.clone(),
                worker_queue: event.worker_queue.clone(),
                dispatch_id: event.dispatch_id.clone(),
            }
        }
        orchestration_event_envelope::Event::TimerRequested(event) => {
            OrchestrationEventData::TimerRequested {
                timer_id: event.timer_id.clone(),
                timer_type: proto_timer_type_to_runtime(event.timer_type)?,
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                fire_at: protobuf_to_chrono(event.fire_at.as_ref(), "timer_requested.fire_at")?,
            }
        }
        orchestration_event_envelope::Event::DispatchEnqueued(event) => {
            OrchestrationEventData::DispatchEnqueued {
                dispatch_id: event.dispatch_id.clone(),
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                cloud_task_id: event.cloud_task_id.clone(),
            }
        }
        orchestration_event_envelope::Event::TimerEnqueued(event) => {
            OrchestrationEventData::TimerEnqueued {
                timer_id: event.timer_id.clone(),
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
                cloud_task_id: event.cloud_task_id.clone(),
            }
        }
        orchestration_event_envelope::Event::TimerFired(event) => {
            OrchestrationEventData::TimerFired {
                timer_id: event.timer_id.clone(),
                timer_type: proto_timer_type_to_runtime(event.timer_type)?,
                run_id: event.run_id.clone(),
                task_key: event.task_key.clone(),
                attempt: event.attempt,
            }
        }
        orchestration_event_envelope::Event::ScheduleDefinitionUpserted(event) => {
            OrchestrationEventData::ScheduleDefinitionUpserted {
                schedule_id: event.schedule_id.clone(),
                cron_expression: event.cron_expression.clone(),
                timezone: event.timezone.clone(),
                catchup_window_minutes: event.catchup_window_minutes,
                asset_selection: event.asset_selection.clone(),
                code_version: event.code_version.clone(),
                max_catchup_ticks: event.max_catchup_ticks,
                enabled: event.enabled,
            }
        }
        orchestration_event_envelope::Event::ScheduleTicked(event) => {
            OrchestrationEventData::ScheduleTicked {
                schedule_id: event.schedule_id.clone(),
                scheduled_for: protobuf_to_chrono(
                    event.scheduled_for.as_ref(),
                    "schedule_ticked.scheduled_for",
                )?,
                tick_id: event.tick_id.clone(),
                definition_version: event.definition_version.clone(),
                asset_selection: event.asset_selection.clone(),
                partition_selection: (!event.partition_selection.is_empty())
                    .then(|| event.partition_selection.clone()),
                status: proto_tick_status_to_runtime(
                    event.status,
                    event.skipped_reason.as_deref(),
                    event.failure_message.as_deref(),
                )?,
                run_key: event.run_key.clone(),
                request_fingerprint: event.request_fingerprint.clone(),
            }
        }
        orchestration_event_envelope::Event::SensorEvaluated(event) => {
            OrchestrationEventData::SensorEvaluated {
                sensor_id: event.sensor_id.clone(),
                eval_id: event.eval_id.clone(),
                cursor_before: event.cursor_before.clone(),
                cursor_after: event.cursor_after.clone(),
                expected_state_version: event.expected_state_version,
                trigger_source: proto_trigger_source_to_runtime(
                    event.trigger_source.as_ref(),
                    "sensor_evaluated.trigger_source",
                )?,
                run_requests: event
                    .run_requests
                    .iter()
                    .map(run_request_from_proto)
                    .collect(),
                status: proto_sensor_eval_status_to_runtime(
                    event.status,
                    event.error_message.as_deref(),
                )?,
            }
        }
        orchestration_event_envelope::Event::RunRequested(event) => {
            OrchestrationEventData::RunRequested {
                run_key: event.run_key.clone(),
                request_fingerprint: event.request_fingerprint.clone(),
                asset_selection: event.asset_selection.clone(),
                partition_selection: (!event.partition_selection.is_empty())
                    .then(|| event.partition_selection.clone()),
                trigger_source_ref: proto_trigger_info_to_source_ref(
                    event.trigger.as_ref(),
                    "run_requested.trigger",
                )?,
                labels: btree_to_hash(&event.labels),
                code_version: event.code_version.clone(),
            }
        }
        orchestration_event_envelope::Event::BackfillCreated(event) => {
            OrchestrationEventData::BackfillCreated {
                backfill_id: event.backfill_id.clone(),
                client_request_id: event.client_request_id.clone(),
                asset_selection: event.asset_selection.clone(),
                code_version: event.code_version.clone(),
                partition_selector: partition_selector_from_proto(
                    event.partition_selector.as_ref(),
                )?,
                total_partitions: event.total_partitions,
                chunk_size: event.chunk_size,
                max_concurrent_runs: event.max_concurrent_runs,
                parent_backfill_id: event.parent_backfill_id.clone(),
            }
        }
        orchestration_event_envelope::Event::BackfillChunkPlanned(event) => {
            OrchestrationEventData::BackfillChunkPlanned {
                backfill_id: event.backfill_id.clone(),
                chunk_id: event.chunk_id.clone(),
                chunk_index: event.chunk_index,
                partition_keys: event.partition_keys.clone(),
                run_key: event.run_key.clone(),
                request_fingerprint: event.request_fingerprint.clone(),
            }
        }
        orchestration_event_envelope::Event::BackfillStateChanged(event) => {
            OrchestrationEventData::BackfillStateChanged {
                backfill_id: event.backfill_id.clone(),
                from_state: proto_backfill_state_to_runtime(event.from_state)?,
                to_state: proto_backfill_state_to_runtime(event.to_state)?,
                state_version: event.state_version,
                changed_by: event.changed_by.clone(),
            }
        }
    })
}

fn callback_output_to_proto(output: &CallbackTaskOutput) -> proto::TaskCallbackOutput {
    proto::TaskCallbackOutput {
        materialization_id: output.materialization_id.clone(),
        row_count: output.row_count,
        byte_size: output.byte_size,
        output_path: output.output_path.clone(),
        delta_table: output.delta_table.clone(),
        delta_version: output.delta_version,
        delta_partition: output.delta_partition.clone(),
    }
}

fn callback_output_from_proto(output: &proto::TaskCallbackOutput) -> CallbackTaskOutput {
    CallbackTaskOutput {
        materialization_id: output.materialization_id.clone(),
        row_count: output.row_count,
        byte_size: output.byte_size,
        output_path: output.output_path.clone(),
        delta_table: output.delta_table.clone(),
        delta_version: output.delta_version,
        delta_partition: output.delta_partition.clone(),
        output_visibility_state: None,
        published_at: None,
        publish_error: None,
    }
}

fn callback_error_to_proto(error: &CallbackTaskError) -> proto::TaskError {
    proto::TaskError {
        category: callback_error_category_to_proto(error.category) as i32,
        message: error.message.clone(),
        detail: error.stack_trace.clone(),
        retryable: error.retryable,
    }
}

fn callback_error_from_proto(
    error: &proto::TaskError,
) -> Result<CallbackTaskError, OrchestrationProtoError> {
    Ok(CallbackTaskError {
        category: proto_callback_error_category_to_runtime(error.category)?,
        message: error.message.clone(),
        stack_trace: error.detail.clone(),
        retryable: error.retryable,
    })
}

fn callback_metrics_to_proto(metrics: &CallbackTaskMetrics) -> proto::TaskMetrics {
    proto::TaskMetrics {
        cpu_time_ms: metrics.cpu_time_ms,
        peak_memory_bytes: metrics.peak_memory_bytes,
        io_read_bytes: metrics.io_read_bytes,
        io_write_bytes: metrics.io_write_bytes,
    }
}

fn callback_metrics_from_proto(metrics: &proto::TaskMetrics) -> CallbackTaskMetrics {
    CallbackTaskMetrics {
        cpu_time_ms: metrics.cpu_time_ms,
        peak_memory_bytes: metrics.peak_memory_bytes,
        io_read_bytes: metrics.io_read_bytes,
        io_write_bytes: metrics.io_write_bytes,
    }
}

fn task_finished_output_to_proto(
    materialization_id: Option<&str>,
    output: Option<&CallbackTaskOutput>,
) -> Option<proto::TaskCallbackOutput> {
    match output {
        Some(output) => {
            let mut proto_output = callback_output_to_proto(output);
            if proto_output.materialization_id.is_none() {
                proto_output.materialization_id = materialization_id.map(ToString::to_string);
            }
            Some(proto_output)
        }
        None => materialization_id.map(|materialization_id| proto::TaskCallbackOutput {
            materialization_id: Some(materialization_id.to_string()),
            row_count: None,
            byte_size: None,
            output_path: None,
            delta_table: None,
            delta_version: None,
            delta_partition: None,
        }),
    }
}

fn task_finished_error_to_proto(
    error_message: Option<&str>,
    error: Option<&CallbackTaskError>,
) -> Option<proto::TaskError> {
    match error {
        Some(error) => Some(callback_error_to_proto(error)),
        None => error_message.map(|error_message| proto::TaskError {
            category: proto::TaskErrorCategory::Unknown as i32,
            message: error_message.to_string(),
            detail: None,
            retryable: None,
        }),
    }
}

fn task_finished_error_from_proto(
    error: Option<&proto::TaskError>,
) -> Result<(Option<String>, Option<CallbackTaskError>), OrchestrationProtoError> {
    let Some(error) = error else {
        return Ok((None, None));
    };

    match proto::TaskErrorCategory::try_from(error.category).map_err(|_| {
        OrchestrationProtoError::UnknownEnum {
            enum_name: "TaskErrorCategory",
            value: error.category,
        }
    })? {
        proto::TaskErrorCategory::Unspecified | proto::TaskErrorCategory::Unknown => {
            Ok((Some(error.message.clone()), None))
        }
        _ => {
            let error = callback_error_from_proto(error)?;
            Ok((Some(error.message.clone()), Some(error)))
        }
    }
}

#[allow(clippy::single_option_map)]
fn public_output_from_materialization(
    materialization_id: Option<&str>,
) -> Option<proto::TaskOutput> {
    materialization_id.map(|value| proto::TaskOutput {
        materialization_id: Some(value.to_string()),
        files: Vec::new(),
        row_count: None,
        byte_size: None,
        visibility_state: proto::OutputVisibilityState::Pending as i32,
        published_at: None,
        publish_error: None,
    })
}

fn public_output_from_legacy_task_output(output: &LegacyTaskOutput) -> proto::TaskOutput {
    proto::TaskOutput {
        materialization_id: Some(output.materialization_id.to_string()),
        files: Vec::new(),
        row_count: None,
        byte_size: None,
        visibility_state: proto::OutputVisibilityState::Pending as i32,
        published_at: None,
        publish_error: None,
    }
}

fn legacy_task_error_to_proto(error: &LegacyTaskError) -> proto::TaskError {
    proto::TaskError {
        category: legacy_task_error_category_to_proto(error.category) as i32,
        message: error.message.clone(),
        detail: error.detail.clone(),
        retryable: Some(error.retryable),
    }
}

fn task_key_to_proto(task_key: &LegacyTaskKey) -> Result<proto::TaskKey, OrchestrationProtoError> {
    Ok(proto::TaskKey {
        asset_key: Some(task_key.asset_key.canonical_string()),
        partition_key: task_key
            .partition_key
            .as_ref()
            .map(core_partition_key_to_proto),
        operation: legacy_task_operation_to_proto(task_key.operation) as i32,
    })
}

fn legacy_run_trigger_to_proto(trigger: &crate::run::RunTrigger) -> proto::RunTrigger {
    proto::RunTrigger {
        r#type: match trigger.trigger_type {
            TriggerType::Manual => proto::TriggerType::Manual as i32,
            TriggerType::Scheduled => proto::TriggerType::Scheduled as i32,
            TriggerType::Sensor => proto::TriggerType::Sensor as i32,
            TriggerType::Backfill => proto::TriggerType::Backfill as i32,
        },
        triggered_by: trigger.triggered_by.clone(),
        triggered_at: Some(chrono_to_protobuf(trigger.triggered_at)),
        schedule_name: trigger.schedule_name.clone(),
    }
}

fn task_outcome_to_proto(outcome: TaskOutcome) -> proto::TaskOutcome {
    match outcome {
        TaskOutcome::Succeeded => proto::TaskOutcome::Succeeded,
        TaskOutcome::Failed => proto::TaskOutcome::Failed,
        TaskOutcome::Skipped => proto::TaskOutcome::Skipped,
        TaskOutcome::Cancelled => proto::TaskOutcome::Cancelled,
    }
}

fn proto_task_outcome_to_runtime(value: i32) -> Result<TaskOutcome, OrchestrationProtoError> {
    match proto::TaskOutcome::try_from(value).map_err(|_| OrchestrationProtoError::UnknownEnum {
        enum_name: "TaskOutcome",
        value,
    })? {
        proto::TaskOutcome::Unspecified => Err(OrchestrationProtoError::UnknownEnum {
            enum_name: "TaskOutcome",
            value,
        }),
        proto::TaskOutcome::Succeeded => Ok(TaskOutcome::Succeeded),
        proto::TaskOutcome::Failed => Ok(TaskOutcome::Failed),
        proto::TaskOutcome::Skipped => Ok(TaskOutcome::Skipped),
        proto::TaskOutcome::Cancelled => Ok(TaskOutcome::Cancelled),
    }
}

fn output_visibility_state_to_proto(state: OutputVisibilityState) -> proto::OutputVisibilityState {
    match state {
        OutputVisibilityState::Pending => proto::OutputVisibilityState::Pending,
        OutputVisibilityState::Visible => proto::OutputVisibilityState::Visible,
        OutputVisibilityState::Failed => proto::OutputVisibilityState::Failed,
    }
}

fn proto_output_visibility_state_to_runtime(
    value: i32,
) -> Result<OutputVisibilityState, OrchestrationProtoError> {
    match proto::OutputVisibilityState::try_from(value).map_err(|_| {
        OrchestrationProtoError::UnknownEnum {
            enum_name: "OutputVisibilityState",
            value,
        }
    })? {
        proto::OutputVisibilityState::Unspecified => Err(OrchestrationProtoError::UnknownEnum {
            enum_name: "OutputVisibilityState",
            value,
        }),
        proto::OutputVisibilityState::Pending => Ok(OutputVisibilityState::Pending),
        proto::OutputVisibilityState::Visible => Ok(OutputVisibilityState::Visible),
        proto::OutputVisibilityState::Failed => Ok(OutputVisibilityState::Failed),
    }
}

fn timer_type_to_proto(timer_type: TimerType) -> proto::TimerType {
    match timer_type {
        TimerType::Retry => proto::TimerType::Retry,
        TimerType::HeartbeatCheck => proto::TimerType::HeartbeatCheck,
        TimerType::Cron => proto::TimerType::Cron,
        TimerType::SlaCheck => proto::TimerType::SlaCheck,
    }
}

fn proto_timer_type_to_runtime(value: i32) -> Result<TimerType, OrchestrationProtoError> {
    match proto::TimerType::try_from(value).map_err(|_| OrchestrationProtoError::UnknownEnum {
        enum_name: "TimerType",
        value,
    })? {
        proto::TimerType::Unspecified => Err(OrchestrationProtoError::UnknownEnum {
            enum_name: "TimerType",
            value,
        }),
        proto::TimerType::Retry => Ok(TimerType::Retry),
        proto::TimerType::HeartbeatCheck => Ok(TimerType::HeartbeatCheck),
        proto::TimerType::Cron => Ok(TimerType::Cron),
        proto::TimerType::SlaCheck => Ok(TimerType::SlaCheck),
    }
}

fn tick_status_to_proto(
    status: &TickStatus,
) -> (proto::TickStatus, Option<String>, Option<String>) {
    match status {
        TickStatus::Triggered => (proto::TickStatus::Triggered, None, None),
        TickStatus::Skipped { reason } => (proto::TickStatus::Skipped, Some(reason.clone()), None),
        TickStatus::Failed { error } => (proto::TickStatus::Failed, None, Some(error.clone())),
    }
}

fn proto_tick_status_to_runtime(
    value: i32,
    skipped_reason: Option<&str>,
    failure_message: Option<&str>,
) -> Result<TickStatus, OrchestrationProtoError> {
    match proto::TickStatus::try_from(value).map_err(|_| OrchestrationProtoError::UnknownEnum {
        enum_name: "TickStatus",
        value,
    })? {
        proto::TickStatus::Unspecified => Err(OrchestrationProtoError::UnknownEnum {
            enum_name: "TickStatus",
            value,
        }),
        proto::TickStatus::Triggered => Ok(TickStatus::Triggered),
        proto::TickStatus::Skipped => Ok(TickStatus::Skipped {
            reason: skipped_reason.unwrap_or("skipped").to_string(),
        }),
        proto::TickStatus::Failed => Ok(TickStatus::Failed {
            error: failure_message.unwrap_or("tick failed").to_string(),
        }),
    }
}

fn sensor_eval_status_to_proto(
    status: &SensorEvalStatus,
) -> (proto::SensorEvalStatus, Option<String>) {
    match status {
        SensorEvalStatus::Triggered => (proto::SensorEvalStatus::Triggered, None),
        SensorEvalStatus::NoNewData => (proto::SensorEvalStatus::NoNewData, None),
        SensorEvalStatus::Error { message } => {
            (proto::SensorEvalStatus::Error, Some(message.clone()))
        }
        SensorEvalStatus::SkippedStaleCursor => (proto::SensorEvalStatus::SkippedStaleCursor, None),
    }
}

fn proto_sensor_eval_status_to_runtime(
    value: i32,
    error_message: Option<&str>,
) -> Result<SensorEvalStatus, OrchestrationProtoError> {
    match proto::SensorEvalStatus::try_from(value).map_err(|_| {
        OrchestrationProtoError::UnknownEnum {
            enum_name: "SensorEvalStatus",
            value,
        }
    })? {
        proto::SensorEvalStatus::Unspecified => Err(OrchestrationProtoError::UnknownEnum {
            enum_name: "SensorEvalStatus",
            value,
        }),
        proto::SensorEvalStatus::Triggered => Ok(SensorEvalStatus::Triggered),
        proto::SensorEvalStatus::NoNewData => Ok(SensorEvalStatus::NoNewData),
        proto::SensorEvalStatus::Error => Ok(SensorEvalStatus::Error {
            message: error_message
                .unwrap_or("sensor evaluation failed")
                .to_string(),
        }),
        proto::SensorEvalStatus::SkippedStaleCursor => Ok(SensorEvalStatus::SkippedStaleCursor),
    }
}

fn trigger_info_to_proto(
    trigger: &TriggerInfo,
) -> Result<proto::TriggerInfo, OrchestrationProtoError> {
    let trigger = match trigger {
        TriggerInfo::Cron { schedule_id } => {
            trigger_info::Trigger::Schedule(proto::ScheduleTrigger {
                schedule_id: schedule_id.clone(),
                tick_id: None,
            })
        }
        TriggerInfo::Manual { user_id } => trigger_info::Trigger::Manual(proto::ManualTrigger {
            user_id: user_id.clone(),
            request_id: None,
        }),
        TriggerInfo::Materialization {
            upstream_materialization_id,
        } => trigger_info::Trigger::Materialization(proto::MaterializationTrigger {
            upstream_materialization_id: upstream_materialization_id.clone(),
        }),
        TriggerInfo::Webhook { webhook_id } => {
            trigger_info::Trigger::Webhook(proto::WebhookTrigger {
                webhook_id: webhook_id.clone(),
            })
        }
        TriggerInfo::Sensor { sensor_id, cursor } => {
            trigger_info::Trigger::Sensor(proto::SensorTrigger {
                sensor_id: sensor_id.clone(),
                cursor: (!cursor.is_empty()).then(|| cursor.clone()),
                eval_id: None,
            })
        }
    };
    Ok(proto::TriggerInfo {
        trigger: Some(trigger),
    })
}

fn proto_trigger_info_to_runtime(
    trigger: Option<&proto::TriggerInfo>,
    context: &'static str,
) -> Result<TriggerInfo, OrchestrationProtoError> {
    let trigger = trigger.and_then(|value| value.trigger.as_ref()).ok_or(
        OrchestrationProtoError::UnsupportedTriggerKind {
            context,
            kind: "missing",
        },
    )?;

    match trigger {
        trigger_info::Trigger::Manual(manual) => Ok(TriggerInfo::Manual {
            user_id: manual.user_id.clone(),
        }),
        trigger_info::Trigger::Schedule(schedule) => Ok(TriggerInfo::Cron {
            schedule_id: schedule.schedule_id.clone(),
        }),
        trigger_info::Trigger::Materialization(materialization) => {
            Ok(TriggerInfo::Materialization {
                upstream_materialization_id: materialization.upstream_materialization_id.clone(),
            })
        }
        trigger_info::Trigger::Webhook(webhook) => Ok(TriggerInfo::Webhook {
            webhook_id: webhook.webhook_id.clone(),
        }),
        trigger_info::Trigger::Sensor(sensor) => Ok(TriggerInfo::Sensor {
            sensor_id: sensor.sensor_id.clone(),
            cursor: sensor.cursor.clone().unwrap_or_default(),
        }),
        trigger_info::Trigger::Backfill(_) => {
            Err(OrchestrationProtoError::UnsupportedTriggerKind {
                context,
                kind: "backfill",
            })
        }
    }
}

fn source_ref_to_proto(source_ref: &SourceRef) -> proto::TriggerInfo {
    let trigger = match source_ref {
        SourceRef::Schedule {
            schedule_id,
            tick_id,
        } => trigger_info::Trigger::Schedule(proto::ScheduleTrigger {
            schedule_id: schedule_id.clone(),
            tick_id: Some(tick_id.clone()),
        }),
        SourceRef::Sensor { sensor_id, eval_id } => {
            trigger_info::Trigger::Sensor(proto::SensorTrigger {
                sensor_id: sensor_id.clone(),
                cursor: None,
                eval_id: Some(eval_id.clone()),
            })
        }
        SourceRef::Backfill {
            backfill_id,
            chunk_id,
        } => trigger_info::Trigger::Backfill(proto::BackfillTrigger {
            backfill_id: backfill_id.clone(),
            chunk_id: Some(chunk_id.clone()),
        }),
        SourceRef::Manual {
            user_id,
            request_id,
        } => trigger_info::Trigger::Manual(proto::ManualTrigger {
            user_id: user_id.clone(),
            request_id: Some(request_id.clone()),
        }),
    };

    proto::TriggerInfo {
        trigger: Some(trigger),
    }
}

fn proto_trigger_info_to_source_ref(
    trigger: Option<&proto::TriggerInfo>,
    context: &'static str,
) -> Result<SourceRef, OrchestrationProtoError> {
    let trigger = trigger.and_then(|value| value.trigger.as_ref()).ok_or(
        OrchestrationProtoError::UnsupportedTriggerKind {
            context,
            kind: "missing",
        },
    )?;

    match trigger {
        trigger_info::Trigger::Manual(manual) => Ok(SourceRef::Manual {
            user_id: manual.user_id.clone(),
            request_id: manual.request_id.clone().unwrap_or_default(),
        }),
        trigger_info::Trigger::Schedule(schedule) => Ok(SourceRef::Schedule {
            schedule_id: schedule.schedule_id.clone(),
            tick_id: schedule.tick_id.clone().unwrap_or_default(),
        }),
        trigger_info::Trigger::Sensor(sensor) => Ok(SourceRef::Sensor {
            sensor_id: sensor.sensor_id.clone(),
            eval_id: sensor.eval_id.clone().unwrap_or_default(),
        }),
        trigger_info::Trigger::Backfill(backfill) => Ok(SourceRef::Backfill {
            backfill_id: backfill.backfill_id.clone(),
            chunk_id: backfill.chunk_id.clone().unwrap_or_default(),
        }),
        trigger_info::Trigger::Materialization(_) => {
            Err(OrchestrationProtoError::UnsupportedTriggerKind {
                context,
                kind: "materialization",
            })
        }
        trigger_info::Trigger::Webhook(_) => Err(OrchestrationProtoError::UnsupportedTriggerKind {
            context,
            kind: "webhook",
        }),
    }
}

fn trigger_source_to_proto(trigger_source: &TriggerSource) -> proto::TriggerSource {
    let source = match trigger_source {
        TriggerSource::Push { message_id } => {
            proto::trigger_source::Source::Push(proto::PushTriggerSource {
                message_id: message_id.clone(),
            })
        }
        TriggerSource::Poll { poll_epoch } => {
            proto::trigger_source::Source::Poll(proto::PollTriggerSource {
                poll_epoch: *poll_epoch,
            })
        }
    };
    proto::TriggerSource {
        source: Some(source),
    }
}

fn proto_trigger_source_to_runtime(
    trigger_source: Option<&proto::TriggerSource>,
    context: &'static str,
) -> Result<TriggerSource, OrchestrationProtoError> {
    let source = trigger_source
        .and_then(|value| value.source.as_ref())
        .ok_or(OrchestrationProtoError::UnsupportedTriggerKind {
            context,
            kind: "missing",
        })?;

    Ok(match source {
        proto::trigger_source::Source::Push(push) => TriggerSource::Push {
            message_id: push.message_id.clone(),
        },
        proto::trigger_source::Source::Poll(poll) => TriggerSource::Poll {
            poll_epoch: poll.poll_epoch,
        },
    })
}

fn run_request_to_proto(request: &RunRequest) -> proto::RunRequest {
    proto::RunRequest {
        run_key: request.run_key.clone(),
        request_fingerprint: request.request_fingerprint.clone(),
        asset_selection: request.asset_selection.clone(),
        partition_selection: request.partition_selection.clone().unwrap_or_default(),
        code_version: request.code_version.clone(),
    }
}

fn run_request_from_proto(request: &proto::RunRequest) -> RunRequest {
    RunRequest {
        run_key: request.run_key.clone(),
        request_fingerprint: request.request_fingerprint.clone(),
        asset_selection: request.asset_selection.clone(),
        partition_selection: (!request.partition_selection.is_empty())
            .then(|| request.partition_selection.clone()),
        code_version: request.code_version.clone(),
    }
}

fn task_def_to_proto(task: &TaskDef) -> Result<proto::TaskDef, OrchestrationProtoError> {
    Ok(proto::TaskDef {
        key: task.key.clone(),
        depends_on: task.depends_on.clone(),
        asset_key: task.asset_key.clone(),
        partition_key: string_to_proto_partition_key(task.partition_key.as_deref())?,
        max_attempts: task.max_attempts,
        heartbeat_timeout_sec: task.heartbeat_timeout_sec,
    })
}

fn proto_task_def_to_runtime(task: &proto::TaskDef) -> Result<TaskDef, OrchestrationProtoError> {
    Ok(TaskDef {
        key: task.key.clone(),
        depends_on: task.depends_on.clone(),
        asset_key: task.asset_key.clone(),
        partition_key: proto_partition_key_to_string(task.partition_key.as_ref())?,
        max_attempts: task.max_attempts,
        heartbeat_timeout_sec: task.heartbeat_timeout_sec,
        requires_visible_output: false,
    })
}

fn partition_selector_to_proto(selector: &PartitionSelector) -> proto::PartitionSelector {
    let selector = match selector {
        PartitionSelector::Range { start, end } => {
            proto::partition_selector::Selector::Range(proto::PartitionRangeSelector {
                start: start.clone(),
                end: end.clone(),
            })
        }
        PartitionSelector::Explicit { partition_keys } => {
            proto::partition_selector::Selector::Explicit(proto::ExplicitPartitionSelector {
                partition_keys: partition_keys.clone(),
            })
        }
        PartitionSelector::Filter { filters } => {
            proto::partition_selector::Selector::Filter(proto::FilterPartitionSelector {
                filters: hash_to_btree(filters),
            })
        }
    };
    proto::PartitionSelector {
        selector: Some(selector),
    }
}

fn partition_selector_from_proto(
    selector: Option<&proto::PartitionSelector>,
) -> Result<PartitionSelector, OrchestrationProtoError> {
    let selector = selector.and_then(|value| value.selector.as_ref()).ok_or(
        OrchestrationProtoError::UnsupportedTriggerKind {
            context: "partition_selector",
            kind: "missing",
        },
    )?;

    Ok(match selector {
        proto::partition_selector::Selector::Range(range) => PartitionSelector::Range {
            start: range.start.clone(),
            end: range.end.clone(),
        },
        proto::partition_selector::Selector::Explicit(explicit) => PartitionSelector::Explicit {
            partition_keys: explicit.partition_keys.clone(),
        },
        proto::partition_selector::Selector::Filter(filter) => PartitionSelector::Filter {
            filters: btree_to_hash(&filter.filters),
        },
    })
}

fn backfill_state_to_proto(state: BackfillState) -> proto::BackfillState {
    match state {
        BackfillState::Pending => proto::BackfillState::Pending,
        BackfillState::Running => proto::BackfillState::Running,
        BackfillState::Paused => proto::BackfillState::Paused,
        BackfillState::Succeeded => proto::BackfillState::Succeeded,
        BackfillState::Failed => proto::BackfillState::Failed,
        BackfillState::Cancelled => proto::BackfillState::Cancelled,
    }
}

fn proto_backfill_state_to_runtime(value: i32) -> Result<BackfillState, OrchestrationProtoError> {
    match proto::BackfillState::try_from(value).map_err(|_| {
        OrchestrationProtoError::UnknownEnum {
            enum_name: "BackfillState",
            value,
        }
    })? {
        proto::BackfillState::Unspecified => Err(OrchestrationProtoError::UnknownEnum {
            enum_name: "BackfillState",
            value,
        }),
        proto::BackfillState::Pending => Ok(BackfillState::Pending),
        proto::BackfillState::Running => Ok(BackfillState::Running),
        proto::BackfillState::Paused => Ok(BackfillState::Paused),
        proto::BackfillState::Succeeded => Ok(BackfillState::Succeeded),
        proto::BackfillState::Failed => Ok(BackfillState::Failed),
        proto::BackfillState::Cancelled => Ok(BackfillState::Cancelled),
    }
}

#[allow(dead_code)]
fn chunk_state_to_proto(state: ChunkState) -> proto::ChunkState {
    match state {
        ChunkState::Pending => proto::ChunkState::Pending,
        ChunkState::Planned => proto::ChunkState::Planned,
        ChunkState::Running => proto::ChunkState::Running,
        ChunkState::Succeeded => proto::ChunkState::Succeeded,
        ChunkState::Failed => proto::ChunkState::Failed,
    }
}

fn legacy_task_operation_to_proto(operation: LegacyTaskOperation) -> proto::TaskOperation {
    match operation {
        LegacyTaskOperation::Materialize => proto::TaskOperation::Materialize,
        LegacyTaskOperation::Check => proto::TaskOperation::Check,
        LegacyTaskOperation::Backfill => proto::TaskOperation::Unspecified,
    }
}

fn callback_error_category_to_proto(category: CallbackErrorCategory) -> proto::TaskErrorCategory {
    match category {
        CallbackErrorCategory::UserCode => proto::TaskErrorCategory::UserCode,
        CallbackErrorCategory::DataQuality => proto::TaskErrorCategory::DataQuality,
        CallbackErrorCategory::Infrastructure => proto::TaskErrorCategory::Infrastructure,
        CallbackErrorCategory::Configuration => proto::TaskErrorCategory::Configuration,
        CallbackErrorCategory::Timeout => proto::TaskErrorCategory::Timeout,
        CallbackErrorCategory::Cancelled => proto::TaskErrorCategory::Cancelled,
    }
}

fn proto_callback_error_category_to_runtime(
    value: i32,
) -> Result<CallbackErrorCategory, OrchestrationProtoError> {
    match proto::TaskErrorCategory::try_from(value).map_err(|_| {
        OrchestrationProtoError::UnknownEnum {
            enum_name: "TaskErrorCategory",
            value,
        }
    })? {
        proto::TaskErrorCategory::Unspecified | proto::TaskErrorCategory::Unknown => {
            Err(OrchestrationProtoError::UnknownEnum {
                enum_name: "TaskErrorCategory",
                value,
            })
        }
        proto::TaskErrorCategory::UserCode => Ok(CallbackErrorCategory::UserCode),
        proto::TaskErrorCategory::DataQuality => Ok(CallbackErrorCategory::DataQuality),
        proto::TaskErrorCategory::Infrastructure => Ok(CallbackErrorCategory::Infrastructure),
        proto::TaskErrorCategory::Configuration => Ok(CallbackErrorCategory::Configuration),
        proto::TaskErrorCategory::Timeout => Ok(CallbackErrorCategory::Timeout),
        proto::TaskErrorCategory::Cancelled => Ok(CallbackErrorCategory::Cancelled),
    }
}

fn legacy_task_error_category_to_proto(
    category: LegacyTaskErrorCategory,
) -> proto::TaskErrorCategory {
    match category {
        LegacyTaskErrorCategory::UserCode => proto::TaskErrorCategory::UserCode,
        LegacyTaskErrorCategory::DataQuality => proto::TaskErrorCategory::DataQuality,
        LegacyTaskErrorCategory::Infrastructure => proto::TaskErrorCategory::Infrastructure,
        LegacyTaskErrorCategory::Configuration => proto::TaskErrorCategory::Configuration,
        LegacyTaskErrorCategory::Unknown => proto::TaskErrorCategory::Unknown,
    }
}

fn hash_to_btree(values: &HashMap<String, String>) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn btree_to_hash(values: &BTreeMap<String, String>) -> HashMap<String, String> {
    values
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn chrono_to_protobuf(timestamp: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: timestamp.timestamp(),
        nanos: timestamp.timestamp_subsec_nanos() as i32,
    }
}

fn protobuf_to_chrono(
    timestamp: Option<&Timestamp>,
    field: &'static str,
) -> Result<DateTime<Utc>, OrchestrationProtoError> {
    let timestamp = timestamp.ok_or(OrchestrationProtoError::MissingTimestamp { field })?;
    DateTime::<Utc>::from_timestamp(
        timestamp.seconds,
        u32::try_from(timestamp.nanos)
            .map_err(|_| OrchestrationProtoError::InvalidTimestamp { field })?,
    )
    .ok_or(OrchestrationProtoError::InvalidTimestamp { field })
}

fn string_to_proto_partition_key(
    partition_key: Option<&str>,
) -> Result<Option<common_proto::PartitionKey>, OrchestrationProtoError> {
    partition_key
        .filter(|value| !value.is_empty())
        .map(|value| {
            CorePartitionKey::parse(value)
                .map_err(|source| OrchestrationProtoError::InvalidPartitionKey {
                    value: value.to_string(),
                    source,
                })
                .map(|parsed| core_partition_key_to_proto(&parsed))
        })
        .transpose()
}

fn proto_partition_key_to_string(
    partition_key: Option<&common_proto::PartitionKey>,
) -> Result<Option<String>, OrchestrationProtoError> {
    let Some(partition_key) = partition_key else {
        return Ok(None);
    };
    if partition_key.dimensions.is_empty() {
        return Ok(None);
    }

    let mut canonical = CorePartitionKey::new();
    for dimension in &partition_key.dimensions {
        canonical.insert(
            dimension.name.clone(),
            proto_scalar_to_core(dimension.value.as_ref().ok_or(
                OrchestrationProtoError::UnsupportedTriggerKind {
                    context: "partition_dimension",
                    kind: "missing_value",
                },
            )?)
            .ok_or(OrchestrationProtoError::UnsupportedTriggerKind {
                context: "partition_dimension",
                kind: "missing_scalar",
            })?,
        );
    }

    Ok(Some(canonical.canonical_string()))
}

fn core_partition_key_to_proto(partition_key: &CorePartitionKey) -> common_proto::PartitionKey {
    common_proto::PartitionKey {
        dimensions: partition_key
            .iter()
            .map(|(name, value)| common_proto::PartitionDimension {
                name: name.clone(),
                value: Some(core_scalar_to_proto(value)),
            })
            .collect(),
    }
}

fn core_scalar_to_proto(value: &CoreScalarValue) -> common_proto::ScalarValue {
    let value = match value {
        CoreScalarValue::String(value) => scalar_value::Value::StringValue(value.clone()),
        CoreScalarValue::Int64(value) => scalar_value::Value::Int64Value(*value),
        CoreScalarValue::Boolean(value) => scalar_value::Value::BoolValue(*value),
        CoreScalarValue::Date(value) => scalar_value::Value::DateValue(value.clone()),
        CoreScalarValue::Timestamp(value) => scalar_value::Value::TimestampValue(value.clone()),
        CoreScalarValue::Null => scalar_value::Value::NullValue(0),
    };

    common_proto::ScalarValue { value: Some(value) }
}

fn proto_scalar_to_core(value: &common_proto::ScalarValue) -> Option<CoreScalarValue> {
    match value.value.as_ref()? {
        scalar_value::Value::StringValue(value) => Some(CoreScalarValue::String(value.clone())),
        scalar_value::Value::Int64Value(value) => Some(CoreScalarValue::Int64(*value)),
        scalar_value::Value::BoolValue(value) => Some(CoreScalarValue::Boolean(*value)),
        scalar_value::Value::DateValue(value) => Some(CoreScalarValue::Date(value.clone())),
        scalar_value::Value::TimestampValue(value) => {
            Some(CoreScalarValue::Timestamp(value.clone()))
        }
        scalar_value::Value::NullValue(_) => Some(CoreScalarValue::Null),
    }
}
