//! Integration tests for the run request bridge controller.

use std::collections::HashMap;

use chrono::Utc;

use arco_flow::orchestration::compactor::fold::{
    FoldState, RunKeyConflictRow, RunKeyIndexRow, RunRow, RunState, ScheduleTickRow, SensorEvalRow,
};
use arco_flow::orchestration::compactor::manifest::OrchestrationManifest;
use arco_flow::orchestration::controllers::{RunBridgeAction, RunBridgeController};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, RunRequest, SensorEvalStatus, TickStatus,
    TriggerInfo, TriggerSource,
};

fn fresh_manifest() -> OrchestrationManifest {
    let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
    manifest.watermarks.last_processed_at = Utc::now();
    manifest
}

fn run_key_index_row(run_key: &str, run_id: &str, request_fingerprint: &str) -> RunKeyIndexRow {
    RunKeyIndexRow {
        tenant_id: "tenant".to_string(),
        workspace_id: "workspace".to_string(),
        run_key: run_key.to_string(),
        run_id: run_id.to_string(),
        request_fingerprint: request_fingerprint.to_string(),
        created_at: Utc::now(),
        row_version: "evt_01".to_string(),
    }
}

fn schedule_tick_row(
    run_key: &str,
    request_fingerprint: &str,
    asset_selection: Vec<String>,
) -> ScheduleTickRow {
    ScheduleTickRow {
        tenant_id: "tenant".to_string(),
        workspace_id: "workspace".to_string(),
        tick_id: "sched-01:1736935200".to_string(),
        schedule_id: "sched-01".to_string(),
        scheduled_for: Utc::now(),
        definition_version: "v1".to_string(),
        asset_selection,
        partition_selection: None,
        status: TickStatus::Triggered,
        run_key: Some(run_key.to_string()),
        run_id: None,
        request_fingerprint: Some(request_fingerprint.to_string()),
        row_version: "evt_02".to_string(),
    }
}

fn sensor_eval_row(
    eval_id: &str,
    sensor_id: &str,
    run_key: &str,
    request_fingerprint: &str,
    asset_selection: Vec<String>,
) -> SensorEvalRow {
    SensorEvalRow {
        tenant_id: "tenant".to_string(),
        workspace_id: "workspace".to_string(),
        eval_id: eval_id.to_string(),
        sensor_id: sensor_id.to_string(),
        cursor_before: Some("cursor-before".to_string()),
        cursor_after: Some("cursor-after".to_string()),
        expected_state_version: Some(1),
        trigger_source: TriggerSource::Push {
            message_id: "msg-1".to_string(),
        },
        run_requests: vec![RunRequest {
            run_key: run_key.to_string(),
            request_fingerprint: request_fingerprint.to_string(),
            asset_selection,
            partition_selection: None,
        }],
        status: SensorEvalStatus::Triggered,
        evaluated_at: Utc::now(),
        row_version: format!("evt_{eval_id}"),
    }
}

fn existing_run(run_id: &str) -> RunRow {
    RunRow {
        run_id: run_id.to_string(),
        plan_id: "plan-existing".to_string(),
        state: RunState::Triggered,
        run_key: Some("sched:daily:1".to_string()),
        labels: HashMap::new(),
        code_version: None,
        cancel_requested: false,
        tasks_total: 1,
        tasks_completed: 0,
        tasks_succeeded: 0,
        tasks_failed: 0,
        tasks_skipped: 0,
        tasks_cancelled: 0,
        triggered_at: Utc::now(),
        completed_at: None,
        row_version: "evt_existing".to_string(),
    }
}

#[test]
fn run_bridge_emits_run_triggered_and_plan_created_from_schedule_tick() {
    let mut state = FoldState::new();
    state.run_key_index.insert(
        "sched:daily:1".to_string(),
        run_key_index_row("sched:daily:1", "run_sched_1", "fp-1"),
    );
    state.schedule_ticks.insert(
        "sched-01:1736935200".to_string(),
        schedule_tick_row("sched:daily:1", "fp-1", vec!["analytics.daily".to_string()]),
    );

    let controller = RunBridgeController::with_defaults();
    let actions = controller.reconcile(&fresh_manifest(), &state);
    assert_eq!(actions.len(), 1, "expected one run bridge action");

    let action = actions.first().expect("action");
    let RunBridgeAction::EmitRunEvents {
        run_triggered,
        plan_created,
    } = action
    else {
        panic!("expected EmitRunEvents action");
    };

    match run_triggered {
        OrchestrationEventData::RunTriggered {
            run_id,
            trigger,
            root_assets,
            run_key,
            ..
        } => {
            assert_eq!(run_id, "run_sched_1");
            assert_eq!(root_assets.as_slice(), ["analytics.daily".to_string()]);
            assert_eq!(run_key.as_deref(), Some("sched:daily:1"));
            assert!(matches!(
                trigger,
                TriggerInfo::Cron { schedule_id } if schedule_id == "sched-01"
            ));
        }
        _ => panic!("expected RunTriggered event"),
    }

    match plan_created {
        OrchestrationEventData::PlanCreated {
            run_id,
            plan_id,
            tasks,
        } => {
            assert_eq!(run_id, "run_sched_1");
            assert_eq!(plan_id, "plan:run_sched_1");
            assert_eq!(tasks.len(), 1, "expected one selected asset task");
            assert_eq!(tasks[0].asset_key.as_deref(), Some("analytics.daily"));
        }
        _ => panic!("expected PlanCreated event"),
    }
}

#[test]
fn run_bridge_skips_when_run_already_exists() {
    let mut state = FoldState::new();
    state.run_key_index.insert(
        "sched:daily:1".to_string(),
        run_key_index_row("sched:daily:1", "run_sched_1", "fp-1"),
    );
    state.schedule_ticks.insert(
        "sched-01:1736935200".to_string(),
        schedule_tick_row("sched:daily:1", "fp-1", vec!["analytics.daily".to_string()]),
    );
    state
        .runs
        .insert("run_sched_1".to_string(), existing_run("run_sched_1"));

    let controller = RunBridgeController::with_defaults();
    let actions = controller.reconcile(&fresh_manifest(), &state);
    assert_eq!(actions.len(), 1);
    assert!(matches!(
        actions.first(),
        Some(RunBridgeAction::Skip { run_key, reason })
            if run_key == "sched:daily:1" && reason == "run_already_exists"
    ));
}

#[test]
fn run_bridge_duplicate_and_conflict_requests_converge_after_first_emit() {
    let mut state = FoldState::new();
    state.run_key_index.insert(
        "sched:daily:1".to_string(),
        run_key_index_row("sched:daily:1", "run_sched_1", "fp-1"),
    );
    state.schedule_ticks.insert(
        "sched-01:1736935200".to_string(),
        schedule_tick_row("sched:daily:1", "fp-1", vec!["analytics.daily".to_string()]),
    );
    state.run_key_conflicts.insert(
        "conflict:sched:daily:1:evt_conflict".to_string(),
        RunKeyConflictRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            run_key: "sched:daily:1".to_string(),
            existing_fingerprint: "fp-1".to_string(),
            conflicting_fingerprint: "fp-2".to_string(),
            conflicting_event_id: "evt_conflict".to_string(),
            detected_at: Utc::now(),
        },
    );

    let controller = RunBridgeController::with_defaults();
    let first_actions = controller.reconcile(&fresh_manifest(), &state);
    assert_eq!(
        first_actions.len(),
        1,
        "first reconcile should emit one run"
    );

    let RunBridgeAction::EmitRunEvents {
        run_triggered,
        plan_created,
    } = first_actions.first().expect("first action")
    else {
        panic!("expected EmitRunEvents");
    };

    state.fold_event(&OrchestrationEvent::new(
        "tenant",
        "workspace",
        run_triggered.clone(),
    ));
    state.fold_event(&OrchestrationEvent::new(
        "tenant",
        "workspace",
        plan_created.clone(),
    ));

    let second_actions = controller.reconcile(&fresh_manifest(), &state);
    assert_eq!(
        second_actions.len(),
        1,
        "second reconcile should produce deterministic skip"
    );
    assert!(matches!(
        second_actions.first(),
        Some(RunBridgeAction::Skip { run_key, reason })
            if run_key == "sched:daily:1" && reason == "run_already_exists"
    ));
}

#[test]
fn run_bridge_uses_matching_fingerprint_when_conflicts_exist() {
    let mut state = FoldState::new();
    state.run_key_index.insert(
        "sched:daily:1".to_string(),
        run_key_index_row("sched:daily:1", "run_sched_1", "fp-win"),
    );

    state.schedule_ticks.insert(
        "sched-01:1736935200".to_string(),
        schedule_tick_row(
            "sched:daily:1",
            "fp-lose",
            vec!["analytics.wrong".to_string()],
        ),
    );
    state.sensor_evals.insert(
        "eval-01".to_string(),
        sensor_eval_row(
            "01",
            "sensor-1",
            "sched:daily:1",
            "fp-win",
            vec!["analytics.correct".to_string()],
        ),
    );
    state.run_key_conflicts.insert(
        "conflict:sched:daily:1:evt_conflict".to_string(),
        RunKeyConflictRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            run_key: "sched:daily:1".to_string(),
            existing_fingerprint: "fp-win".to_string(),
            conflicting_fingerprint: "fp-lose".to_string(),
            conflicting_event_id: "evt_conflict".to_string(),
            detected_at: Utc::now(),
        },
    );

    let controller = RunBridgeController::with_defaults();
    let actions = controller.reconcile(&fresh_manifest(), &state);
    assert_eq!(actions.len(), 1, "expected one run bridge action");

    let action = actions.first().expect("action");
    let RunBridgeAction::EmitRunEvents {
        run_triggered,
        plan_created,
    } = action
    else {
        panic!("expected EmitRunEvents action");
    };

    match run_triggered {
        OrchestrationEventData::RunTriggered {
            run_id,
            trigger,
            root_assets,
            run_key,
            ..
        } => {
            assert_eq!(run_id, "run_sched_1");
            assert_eq!(root_assets.as_slice(), ["analytics.correct".to_string()]);
            assert_eq!(run_key.as_deref(), Some("sched:daily:1"));
            assert!(matches!(
                trigger,
                TriggerInfo::Sensor { sensor_id, .. } if sensor_id == "sensor-1"
            ));
        }
        _ => panic!("expected RunTriggered event"),
    }

    match plan_created {
        OrchestrationEventData::PlanCreated { tasks, .. } => {
            assert_eq!(tasks.len(), 1, "expected one selected asset task");
            assert_eq!(tasks[0].asset_key.as_deref(), Some("analytics.correct"));
        }
        _ => panic!("expected PlanCreated event"),
    }
}

#[test]
fn run_bridge_skips_when_source_fingerprint_does_not_match_index() {
    let mut state = FoldState::new();
    state.run_key_index.insert(
        "sched:daily:1".to_string(),
        run_key_index_row("sched:daily:1", "run_sched_1", "fp-win"),
    );
    state.schedule_ticks.insert(
        "sched-01:1736935200".to_string(),
        schedule_tick_row(
            "sched:daily:1",
            "fp-lose",
            vec!["analytics.daily".to_string()],
        ),
    );

    let controller = RunBridgeController::with_defaults();
    let actions = controller.reconcile(&fresh_manifest(), &state);
    assert_eq!(actions.len(), 1);
    assert!(matches!(
        actions.first(),
        Some(RunBridgeAction::Skip { run_key, reason })
            if run_key == "sched:daily:1" && reason == "run_request_source_not_found"
    ));
}
