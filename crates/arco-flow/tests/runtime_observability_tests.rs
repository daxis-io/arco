//! Integration tests for orchestration runtime config and backlog observability snapshots.

use std::collections::HashMap;
use std::time::Duration as StdDuration;

use chrono::{Duration, Utc};

use arco_flow::orchestration::compactor::fold::{
    DispatchOutboxRow, DispatchStatus, FoldState, RunKeyConflictRow, RunKeyIndexRow, TimerRow,
    TimerState, TimerType,
};
use arco_flow::orchestration::compactor::manifest::OrchestrationManifest;
use arco_flow::orchestration::runtime::{
    OrchestrationBacklogSnapshot, OrchestrationRuntimeConfig, OrchestrationSloSnapshot,
    snapshot_backlog, snapshot_slos,
};

fn run_key_index_row(run_key: &str, run_id: &str) -> RunKeyIndexRow {
    RunKeyIndexRow {
        tenant_id: "tenant".to_string(),
        workspace_id: "workspace".to_string(),
        run_key: run_key.to_string(),
        run_id: run_id.to_string(),
        request_fingerprint: "fp-1".to_string(),
        created_at: Utc::now(),
        row_version: "evt_01".to_string(),
    }
}

fn run_row(
    run_id: &str,
    run_key: &str,
    triggered_at: chrono::DateTime<Utc>,
) -> arco_flow::orchestration::compactor::fold::RunRow {
    arco_flow::orchestration::compactor::fold::RunRow {
        run_id: run_id.to_string(),
        plan_id: format!("plan:{run_id}"),
        state: arco_flow::orchestration::compactor::fold::RunState::Triggered,
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
        triggered_at,
        completed_at: None,
        row_version: format!("evt_run_{run_id}"),
    }
}

#[test]
fn runtime_config_uses_expected_defaults() {
    let vars: HashMap<String, String> = HashMap::new();
    let cfg = OrchestrationRuntimeConfig::from_env_with(|key| vars.get(key).cloned())
        .expect("default config should parse");

    assert_eq!(cfg.max_compaction_lag, Duration::seconds(30));
    assert_eq!(
        cfg.slo_p95_run_requested_to_triggered,
        StdDuration::from_secs(10)
    );
    assert_eq!(cfg.slo_p95_compaction_lag, Duration::seconds(30));
}

#[test]
fn runtime_config_applies_env_overrides() {
    let vars = HashMap::from([
        (
            "ARCO_FLOW_ORCH_MAX_COMPACTION_LAG_SECS".to_string(),
            "45".to_string(),
        ),
        (
            "ARCO_FLOW_ORCH_SLO_P95_RUN_REQUESTED_TO_TRIGGERED_SECS".to_string(),
            "12".to_string(),
        ),
        (
            "ARCO_FLOW_ORCH_SLO_P95_COMPACTION_LAG_SECS".to_string(),
            "20".to_string(),
        ),
    ]);

    let cfg = OrchestrationRuntimeConfig::from_env_with(|key| vars.get(key).cloned())
        .expect("override config should parse");

    assert_eq!(cfg.max_compaction_lag, Duration::seconds(45));
    assert_eq!(
        cfg.slo_p95_run_requested_to_triggered,
        StdDuration::from_secs(12)
    );
    assert_eq!(cfg.slo_p95_compaction_lag, Duration::seconds(20));
}

#[test]
fn runtime_config_rejects_non_positive_values() {
    let vars = HashMap::from([(
        "ARCO_FLOW_ORCH_MAX_COMPACTION_LAG_SECS".to_string(),
        "0".to_string(),
    )]);

    let err = OrchestrationRuntimeConfig::from_env_with(|key| vars.get(key).cloned())
        .expect_err("non-positive values must be rejected");
    let message = err.to_string();
    assert!(
        message.contains("ARCO_FLOW_ORCH_MAX_COMPACTION_LAG_SECS"),
        "error should mention invalid key: {message}"
    );
}

#[test]
fn backlog_snapshot_reports_pending_counts_conflicts_and_lag() {
    let mut state = FoldState::new();
    state.run_key_index.insert(
        "sched:daily:1".to_string(),
        run_key_index_row("sched:daily:1", "run_sched_1"),
    );
    state.run_key_index.insert(
        "sched:daily:2".to_string(),
        run_key_index_row("sched:daily:2", "run_sched_2"),
    );
    state.runs.insert(
        "run_sched_1".to_string(),
        arco_flow::orchestration::compactor::fold::RunRow {
            run_id: "run_sched_1".to_string(),
            plan_id: "plan_sched_1".to_string(),
            state: arco_flow::orchestration::compactor::fold::RunState::Triggered,
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
        },
    );
    state.run_key_conflicts.insert(
        "conflict:sched:daily:2:evt_conflict".to_string(),
        RunKeyConflictRow {
            tenant_id: "tenant".to_string(),
            workspace_id: "workspace".to_string(),
            run_key: "sched:daily:2".to_string(),
            existing_fingerprint: "fp-a".to_string(),
            conflicting_fingerprint: "fp-b".to_string(),
            conflicting_event_id: "evt_conflict".to_string(),
            detected_at: Utc::now(),
        },
    );

    let now = Utc::now();
    let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
    manifest.watermarks.last_processed_at = now - Duration::seconds(42);

    let snapshot: OrchestrationBacklogSnapshot = snapshot_backlog(now, &manifest, &state);
    assert_eq!(snapshot.run_bridge_pending, 1);
    assert_eq!(snapshot.dispatch_outbox_pending, 0);
    assert_eq!(snapshot.timer_pending, 0);
    assert_eq!(snapshot.run_key_conflicts, 1);
    assert!(
        snapshot.compaction_lag_seconds >= 42.0,
        "expected lag to reflect watermark age"
    );
}

#[test]
fn backlog_snapshot_counts_only_actionable_dispatch_and_timer_rows() {
    let mut state = FoldState::new();
    state.dispatch_outbox.insert(
        "dispatch:pending".to_string(),
        DispatchOutboxRow {
            run_id: "run-1".to_string(),
            task_key: "task_a".to_string(),
            attempt: 1,
            dispatch_id: "dispatch:pending".to_string(),
            cloud_task_id: None,
            status: DispatchStatus::Pending,
            attempt_id: "attempt-1".to_string(),
            worker_queue: "default-queue".to_string(),
            created_at: Utc::now(),
            row_version: "evt_pending".to_string(),
        },
    );
    state.dispatch_outbox.insert(
        "dispatch:acked".to_string(),
        DispatchOutboxRow {
            run_id: "run-1".to_string(),
            task_key: "task_b".to_string(),
            attempt: 1,
            dispatch_id: "dispatch:acked".to_string(),
            cloud_task_id: Some("cloud-acked".to_string()),
            status: DispatchStatus::Acked,
            attempt_id: "attempt-2".to_string(),
            worker_queue: "default-queue".to_string(),
            created_at: Utc::now(),
            row_version: "evt_acked".to_string(),
        },
    );
    state.dispatch_outbox.insert(
        "dispatch:failed".to_string(),
        DispatchOutboxRow {
            run_id: "run-1".to_string(),
            task_key: "task_c".to_string(),
            attempt: 1,
            dispatch_id: "dispatch:failed".to_string(),
            cloud_task_id: None,
            status: DispatchStatus::Failed,
            attempt_id: "attempt-3".to_string(),
            worker_queue: "default-queue".to_string(),
            created_at: Utc::now(),
            row_version: "evt_failed".to_string(),
        },
    );

    state.timers.insert(
        "timer:pending".to_string(),
        TimerRow {
            timer_id: "timer:pending".to_string(),
            cloud_task_id: None,
            timer_type: TimerType::Retry,
            run_id: Some("run-1".to_string()),
            task_key: Some("task_a".to_string()),
            attempt: Some(1),
            fire_at: Utc::now(),
            state: TimerState::Scheduled,
            payload: None,
            row_version: "evt_timer_pending".to_string(),
        },
    );
    state.timers.insert(
        "timer:enqueued".to_string(),
        TimerRow {
            timer_id: "timer:enqueued".to_string(),
            cloud_task_id: Some("cloud-timer".to_string()),
            timer_type: TimerType::Retry,
            run_id: Some("run-1".to_string()),
            task_key: Some("task_a".to_string()),
            attempt: Some(1),
            fire_at: Utc::now(),
            state: TimerState::Scheduled,
            payload: None,
            row_version: "evt_timer_enqueued".to_string(),
        },
    );
    state.timers.insert(
        "timer:fired".to_string(),
        TimerRow {
            timer_id: "timer:fired".to_string(),
            cloud_task_id: None,
            timer_type: TimerType::Retry,
            run_id: Some("run-1".to_string()),
            task_key: Some("task_a".to_string()),
            attempt: Some(1),
            fire_at: Utc::now(),
            state: TimerState::Fired,
            payload: None,
            row_version: "evt_timer_fired".to_string(),
        },
    );

    let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
    manifest.watermarks.last_processed_at = Utc::now();

    let snapshot = snapshot_backlog(Utc::now(), &manifest, &state);
    assert_eq!(
        snapshot.dispatch_outbox_pending, 1,
        "only pending dispatch rows should count toward backlog"
    );
    assert_eq!(
        snapshot.timer_pending, 1,
        "only scheduled timers without cloud_task_id should count toward backlog"
    );
}

#[test]
fn slo_snapshot_detects_compaction_lag_breach() {
    let now = Utc::now();
    let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
    manifest.watermarks.last_processed_at = now - Duration::seconds(42);

    let cfg = OrchestrationRuntimeConfig {
        max_compaction_lag: Duration::seconds(30),
        slo_p95_run_requested_to_triggered: StdDuration::from_secs(10),
        slo_p95_compaction_lag: Duration::seconds(30),
    };

    let state = FoldState::new();
    let snapshot: OrchestrationSloSnapshot = snapshot_slos(now, &manifest, &state, &cfg);
    assert!(
        snapshot.compaction_lag_breached,
        "compaction lag above SLO target should be marked as breach"
    );
    assert!(
        snapshot.run_requested_to_triggered_p95_seconds.is_none(),
        "no completed run pairs should report no observed p95"
    );
}

#[test]
fn slo_snapshot_detects_run_requested_to_triggered_p95_breach() {
    let now = Utc::now();
    let mut manifest = OrchestrationManifest::new("01HQXYZ123REV");
    manifest.watermarks.last_processed_at = now;

    let cfg = OrchestrationRuntimeConfig {
        max_compaction_lag: Duration::seconds(30),
        slo_p95_run_requested_to_triggered: StdDuration::from_secs(10),
        slo_p95_compaction_lag: Duration::seconds(30),
    };

    let mut state = FoldState::new();
    state
        .run_key_index
        .insert("rk1".to_string(), run_key_index_row("rk1", "run_1"));
    state
        .run_key_index
        .insert("rk2".to_string(), run_key_index_row("rk2", "run_2"));

    if let Some(row) = state.run_key_index.get_mut("rk1") {
        row.created_at = now - Duration::seconds(20);
    }
    if let Some(row) = state.run_key_index.get_mut("rk2") {
        row.created_at = now - Duration::seconds(20);
    }

    state.runs.insert(
        "run_1".to_string(),
        run_row("run_1", "rk1", now - Duration::seconds(18)),
    );
    state.runs.insert(
        "run_2".to_string(),
        run_row("run_2", "rk2", now - Duration::seconds(5)),
    );

    let snapshot = snapshot_slos(now, &manifest, &state, &cfg);
    assert!(
        snapshot.run_requested_to_triggered_breached,
        "observed p95 above configured target should be marked as breach"
    );
    assert_eq!(
        snapshot.run_requested_to_triggered_p95_seconds,
        Some(15.0),
        "p95 should match the upper sample for this two-sample case"
    );
}
