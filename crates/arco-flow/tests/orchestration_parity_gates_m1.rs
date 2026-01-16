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
    OrchestrationEvent, OrchestrationEventData, SensorEvalStatus, TickStatus, TriggerSource,
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
