//! Parity gate suite M1: execution invariants + run identity + selection semantics.
//!
//! These tests are intended to be:
//! - hermetic (no external services)
//! - fast (unit/integration style)
//! - hard to “cheat” via doc-only changes

use std::sync::Arc;

use chrono::{TimeZone, Utc};

use arco_core::{MemoryBackend, ScopedStorage};
use arco_flow::error::Result;
use arco_flow::orchestration::compactor::FoldState;
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, SensorEvalStatus, TaskDef, TaskOutcome, TickStatus,
    TriggerInfo, TriggerSource,
};
use arco_flow::orchestration::{
    FingerprintPolicy, ReservationResult, RunKeyReservation, reserve_run_key,
};

fn schedule_ticked_event(
    schedule_id: &str,
    scheduled_for: chrono::DateTime<Utc>,
) -> OrchestrationEvent {
    let tick_id = format!("{schedule_id}:{}", scheduled_for.timestamp());
    OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: schedule_id.to_string(),
            scheduled_for,
            tick_id,
            definition_version: "def_01HQ123".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            status: TickStatus::Triggered,
            run_key: Some(format!("sched:{schedule_id}:{}", scheduled_for.timestamp())),
            request_fingerprint: Some("fp_abc123".into()),
        },
    )
}

fn sensor_eval_push_event(sensor_id: &str, message_id: &str) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: sensor_id.to_string(),
            eval_id: format!("eval_{sensor_id}_{message_id}"),
            cursor_before: None,
            cursor_after: Some(message_id.to_string()),
            expected_state_version: None,
            trigger_source: TriggerSource::Push {
                message_id: message_id.to_string(),
            },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
    )
}

fn make_reservation(run_key: &str, fingerprint: &str) -> RunKeyReservation {
    RunKeyReservation {
        run_key: run_key.to_string(),
        run_id: format!("run_{}", ulid::Ulid::new()),
        plan_id: format!("plan_{}", ulid::Ulid::new()),
        event_id: ulid::Ulid::new().to_string(),
        plan_event_id: Some(ulid::Ulid::new().to_string()),
        request_fingerprint: Some(fingerprint.to_string()),
        created_at: Utc::now(),
    }
}

#[test]
fn parity_m1_schedule_ticked_idempotency_key_uses_tick_id() {
    let scheduled_for = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();
    let event = schedule_ticked_event("01HQ123SCHEDXYZ", scheduled_for);

    assert_eq!(event.event_type, "ScheduleTicked");
    assert_eq!(
        event.idempotency_key,
        "sched_tick:01HQ123SCHEDXYZ:1736935200"
    );
}

#[test]
fn parity_m1_schedule_tick_projection_is_idempotent_under_duplicate_delivery() {
    // Regression guard: fold must ignore duplicate events by idempotency_key, even if the
    // duplicate has a newer event_id.
    let scheduled_for = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();
    let tick_id = format!("daily-etl:{}", scheduled_for.timestamp());

    let mut state = FoldState::new();

    let event1 = schedule_ticked_event("daily-etl", scheduled_for);
    let event2 = schedule_ticked_event("daily-etl", scheduled_for);

    state.fold_event(&event1);
    let row_version_after_first = state
        .schedule_ticks
        .get(&tick_id)
        .expect("tick row should exist")
        .row_version
        .clone();

    state.fold_event(&event2);

    assert_eq!(state.schedule_ticks.len(), 1);
    assert_eq!(state.idempotency_keys.len(), 1);
    assert_eq!(
        state
            .schedule_ticks
            .get(&tick_id)
            .expect("tick row should still exist")
            .row_version,
        row_version_after_first
    );
}

#[test]
fn parity_m1_sensor_push_dedupes_duplicate_message_delivery_in_fold() {
    let mut state = FoldState::new();

    let event1 = sensor_eval_push_event("01HQ123SENSORXYZ", "msg_001");
    let event2 = sensor_eval_push_event("01HQ123SENSORXYZ", "msg_001");

    state.fold_event(&event1);
    state.fold_event(&event2);

    let sensor_state = state
        .sensor_state
        .get("01HQ123SENSORXYZ")
        .expect("sensor state should exist");

    assert_eq!(sensor_state.state_version, 1);
    assert_eq!(state.sensor_evals.len(), 1);
    assert_eq!(state.idempotency_keys.len(), 1);
}

#[tokio::test]
async fn parity_m1_run_key_reservation_duplicate_returns_existing_run() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let reservation1 = make_reservation("run_key:test", "fingerprint-A");
    let result1 = reserve_run_key(&storage, &reservation1, FingerprintPolicy::lenient()).await?;
    assert!(matches!(result1, ReservationResult::Reserved));

    let reservation2 = make_reservation("run_key:test", "fingerprint-A");
    let result2 = reserve_run_key(&storage, &reservation2, FingerprintPolicy::lenient()).await?;

    match result2 {
        ReservationResult::AlreadyExists(existing) => {
            assert_eq!(existing.run_id, reservation1.run_id);
            assert_eq!(existing.plan_id, reservation1.plan_id);
            assert_eq!(
                existing.request_fingerprint,
                reservation1.request_fingerprint
            );
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn parity_m1_run_key_reservation_detects_fingerprint_conflict() -> Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let reservation1 = make_reservation("run_key:test", "fingerprint-A");
    let result1 = reserve_run_key(&storage, &reservation1, FingerprintPolicy::lenient()).await?;
    assert!(matches!(result1, ReservationResult::Reserved));

    let reservation2 = make_reservation("run_key:test", "fingerprint-B");
    let result2 = reserve_run_key(&storage, &reservation2, FingerprintPolicy::lenient()).await?;

    match result2 {
        ReservationResult::FingerprintMismatch {
            existing,
            requested_fingerprint,
        } => {
            assert_eq!(
                existing.request_fingerprint,
                Some("fingerprint-A".to_string())
            );
            assert_eq!(requested_fingerprint, Some("fingerprint-B".to_string()));
        }
        other => panic!("expected FingerprintMismatch, got {other:?}"),
    }

    Ok(())
}

fn run_triggered_event(
    run_id: &str,
    plan_id: &str,
    root_assets: Vec<String>,
) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::RunTriggered {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            trigger: TriggerInfo::Manual {
                user_id: "tester".to_string(),
            },
            root_assets,
            run_key: None,
            labels: std::collections::HashMap::new(),
            code_version: None,
        },
    )
}

fn plan_created_event(run_id: &str, plan_id: &str, tasks: Vec<TaskDef>) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::PlanCreated {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            tasks,
        },
    )
}

fn task_finished_event(
    run_id: &str,
    task_key: &str,
    attempt: u32,
    attempt_id: &str,
    outcome: TaskOutcome,
) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::TaskFinished {
            run_id: run_id.to_string(),
            task_key: task_key.to_string(),
            attempt,
            attempt_id: attempt_id.to_string(),
            worker_id: "worker-1".to_string(),
            outcome,
            materialization_id: None,
            error_message: None,
            output: None,
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
            asset_key: None,
            partition_key: None,
            code_version: None,
        },
    )
}

fn default_task_def(key: &str, depends_on: Vec<&str>) -> TaskDef {
    TaskDef {
        key: key.to_string(),
        depends_on: depends_on.into_iter().map(|dep| dep.to_string()).collect(),
        asset_key: None,
        partition_key: None,
        max_attempts: 3,
        heartbeat_timeout_sec: 60,
    }
}

#[tokio::test]
async fn parity_m1_compactor_persists_out_of_order_dispatch_fields() -> Result<()> {
    use arco_core::WritePrecondition;
    use arco_flow::orchestration::compactor::MicroCompactor;
    use arco_flow::orchestration::compactor::fold::DispatchOutboxRow;
    fn orchestration_event_path(date: &str, event_id: &str) -> String {
        format!("ledger/orchestration/{date}/{event_id}.json")
    }
    use bytes::Bytes;

    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;
    let compactor = MicroCompactor::new(storage.clone());

    let dispatch_id = DispatchOutboxRow::dispatch_id("run_01", "extract", 1);

    let mut enqueued = OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::DispatchEnqueued {
            dispatch_id: dispatch_id.clone(),
            run_id: Some("run_01".to_string()),
            task_key: Some("extract".to_string()),
            attempt: Some(1),
            cloud_task_id: "d_cloud123".to_string(),
        },
    );
    enqueued.event_id = "01B".to_string();
    let path1 = orchestration_event_path("2025-01-15", &enqueued.event_id);
    storage
        .put_raw(
            &path1,
            Bytes::from(serde_json::to_string(&enqueued).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    compactor.compact_events(vec![path1]).await?;

    let mut requested = OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::DispatchRequested {
            run_id: "run_01".to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            attempt_id: "01HQ123ATT".to_string(),
            worker_queue: "default-queue".to_string(),
            dispatch_id: dispatch_id.clone(),
        },
    );
    requested.event_id = "01A".to_string();
    let path2 = orchestration_event_path("2025-01-15", &requested.event_id);
    storage
        .put_raw(
            &path2,
            Bytes::from(serde_json::to_string(&requested).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    compactor.compact_events(vec![path2]).await?;

    let (_, state) = compactor.load_state().await?;
    let row = state.dispatch_outbox.get(&dispatch_id).expect("outbox row");

    assert_eq!(row.attempt_id, "01HQ123ATT");
    assert_eq!(row.cloud_task_id.as_deref(), Some("d_cloud123"));

    Ok(())
}

#[test]
fn parity_m1_duplicate_task_finished_event_is_noop() -> Result<()> {
    let run_id = "run-dup";
    let plan_id = "plan-dup";
    let attempt_id = ulid::Ulid::new().to_string();

    let tasks = vec![default_task_def("extract", vec![])];

    let mut state = FoldState::new();
    state.fold_event(&run_triggered_event(
        run_id,
        plan_id,
        vec!["raw.events".to_string()],
    ));
    state.fold_event(&plan_created_event(run_id, plan_id, tasks));

    let event1 = task_finished_event(run_id, "extract", 1, &attempt_id, TaskOutcome::Succeeded);
    let event2 = task_finished_event(
        run_id,
        "extract",
        1,
        "attempt_other",
        TaskOutcome::Succeeded,
    );

    assert_ne!(event1.event_id, event2.event_id);
    assert_eq!(event1.idempotency_key, event2.idempotency_key);

    state.fold_event(&event1);

    let snapshot = (
        state.runs.clone(),
        state.tasks.clone(),
        state.dep_satisfaction.clone(),
        state.dispatch_outbox.clone(),
        state.timers.clone(),
        state.idempotency_keys.clone(),
    );

    state.fold_event(&event2);

    assert_eq!(snapshot.0, state.runs);
    assert_eq!(snapshot.1, state.tasks);
    assert_eq!(snapshot.2, state.dep_satisfaction);
    assert_eq!(snapshot.3, state.dispatch_outbox);
    assert_eq!(snapshot.4, state.timers);
    assert_eq!(snapshot.5, state.idempotency_keys);

    Ok(())
}
