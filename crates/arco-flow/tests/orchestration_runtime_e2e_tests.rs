//! Runtime orchestration end-to-end tests for automation reconciliation.
//!
//! Focus: `RunRequested` durable intent must be bridged into
//! `RunTriggered` + `PlanCreated` idempotently by the runtime processor.

use std::collections::HashMap;

use chrono::{TimeZone, Utc};

use arco_flow::orchestration::compactor::FoldState;
use arco_flow::orchestration::compactor::fold::{
    RunKeyIndexRow, RunRow, RunState, ScheduleTickRow, SensorEvalRow,
};
use arco_flow::orchestration::controllers::RunRequestProcessor;
use arco_flow::orchestration::events::{
    OrchestrationEventData, RunRequest, SensorEvalStatus, TickStatus, TriggerInfo, TriggerSource,
};

fn seed_run_key_index(
    state: &mut FoldState,
    run_key: &str,
    run_id: &str,
    request_fingerprint: &str,
) {
    state.run_key_index.insert(
        run_key.to_string(),
        RunKeyIndexRow {
            tenant_id: "tenant-abc".to_string(),
            workspace_id: "workspace-prod".to_string(),
            run_key: run_key.to_string(),
            run_id: run_id.to_string(),
            request_fingerprint: request_fingerprint.to_string(),
            created_at: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
            row_version: "evt_runreq_idx".to_string(),
        },
    );
}

#[test]
fn run_request_processor_emits_run_events_for_schedule_source() {
    let run_key = "sched:sched_01:1736935200";
    let run_id = "run_sched_01";
    let request_fingerprint = "fp_sched_01";
    let tick_id = "sched_01:1736935200";

    let mut state = FoldState::new();
    seed_run_key_index(&mut state, run_key, run_id, request_fingerprint);

    state.schedule_ticks.insert(
        tick_id.to_string(),
        ScheduleTickRow {
            tenant_id: "tenant-abc".to_string(),
            workspace_id: "workspace-prod".to_string(),
            tick_id: tick_id.to_string(),
            schedule_id: "sched_01".to_string(),
            scheduled_for: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
            definition_version: "def_v1".to_string(),
            asset_selection: vec!["analytics.daily_summary".to_string()],
            partition_selection: None,
            status: TickStatus::Triggered,
            run_key: Some(run_key.to_string()),
            run_id: None,
            request_fingerprint: Some(request_fingerprint.to_string()),
            row_version: "evt_tick".to_string(),
        },
    );

    let processor = RunRequestProcessor::new();
    let events = processor.reconcile(&state);

    assert_eq!(
        events.len(),
        2,
        "schedule-backed RunRequested should emit RunTriggered + PlanCreated"
    );

    match &events[0].data {
        OrchestrationEventData::RunTriggered {
            run_id: event_run_id,
            run_key: event_run_key,
            trigger,
            root_assets,
            ..
        } => {
            assert_eq!(event_run_id, run_id);
            assert_eq!(event_run_key.as_deref(), Some(run_key));
            assert_eq!(
                root_assets.as_slice(),
                ["analytics.daily_summary".to_string()].as_slice()
            );
            assert!(
                matches!(
                    trigger,
                    TriggerInfo::Cron { schedule_id } if schedule_id == "sched_01"
                ),
                "schedule sources should be emitted as cron triggers"
            );
        }
        other => panic!("expected RunTriggered first, got {other:?}"),
    }

    match &events[1].data {
        OrchestrationEventData::PlanCreated {
            run_id: event_run_id,
            tasks,
            ..
        } => {
            assert_eq!(event_run_id, run_id);
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].key, "analytics.daily_summary");
        }
        other => panic!("expected PlanCreated second, got {other:?}"),
    }
}

#[test]
fn run_request_processor_skips_runs_that_already_exist() {
    let run_key = "sched:sched_02:1736938800";
    let run_id = "run_sched_02";
    let request_fingerprint = "fp_sched_02";

    let mut state = FoldState::new();
    seed_run_key_index(&mut state, run_key, run_id, request_fingerprint);

    state.runs.insert(
        run_id.to_string(),
        RunRow {
            run_id: run_id.to_string(),
            plan_id: "plan_existing".to_string(),
            state: RunState::Running,
            run_key: Some(run_key.to_string()),
            labels: HashMap::new(),
            code_version: None,
            cancel_requested: false,
            tasks_total: 1,
            tasks_completed: 0,
            tasks_succeeded: 0,
            tasks_failed: 0,
            tasks_skipped: 0,
            tasks_cancelled: 0,
            triggered_at: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
            completed_at: None,
            row_version: "evt_existing_run".to_string(),
        },
    );

    let processor = RunRequestProcessor::new();
    let events = processor.reconcile(&state);

    assert!(
        events.is_empty(),
        "runtime should not re-emit run creation for existing runs"
    );
}

#[test]
fn run_request_processor_uses_sensor_eval_request_payload() {
    let run_key = "sensor:sensor_01:poll:cursor_v2";
    let run_id = "run_sensor_01";
    let request_fingerprint = "fp_sensor_01";

    let mut state = FoldState::new();
    seed_run_key_index(&mut state, run_key, run_id, request_fingerprint);

    state.sensor_evals.insert(
        "eval_sensor_01".to_string(),
        SensorEvalRow {
            tenant_id: "tenant-abc".to_string(),
            workspace_id: "workspace-prod".to_string(),
            eval_id: "eval_sensor_01".to_string(),
            sensor_id: "sensor_01".to_string(),
            cursor_before: Some("cursor_v1".to_string()),
            cursor_after: Some("cursor_v2".to_string()),
            expected_state_version: Some(3),
            trigger_source: TriggerSource::Poll {
                poll_epoch: 1_736_935_200,
            },
            run_requests: vec![RunRequest {
                run_key: run_key.to_string(),
                request_fingerprint: request_fingerprint.to_string(),
                asset_selection: vec!["raw.events".to_string()],
                partition_selection: Some(vec!["2025-01-15".to_string()]),
            }],
            status: SensorEvalStatus::Triggered,
            evaluated_at: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
            row_version: "evt_sensor_eval".to_string(),
        },
    );

    let processor = RunRequestProcessor::new();
    let events = processor.reconcile(&state);

    assert_eq!(events.len(), 2);

    match &events[0].data {
        OrchestrationEventData::RunTriggered {
            run_id: event_run_id,
            run_key: event_run_key,
            trigger,
            root_assets,
            ..
        } => {
            assert_eq!(event_run_id, run_id);
            assert_eq!(event_run_key.as_deref(), Some(run_key));
            assert_eq!(
                root_assets.as_slice(),
                ["raw.events".to_string()].as_slice()
            );
            assert!(
                matches!(
                    trigger,
                    TriggerInfo::Sensor { sensor_id, cursor }
                    if sensor_id == "sensor_01" && cursor == "cursor_v2"
                ),
                "sensor sources should preserve sensor_id and cursor context"
            );
        }
        other => panic!("expected RunTriggered first, got {other:?}"),
    }

    match &events[1].data {
        OrchestrationEventData::PlanCreated { tasks, .. } => {
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].partition_key.as_deref(), Some("2025-01-15"));
        }
        other => panic!("expected PlanCreated second, got {other:?}"),
    }
}
