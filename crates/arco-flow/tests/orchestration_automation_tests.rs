//! Tests for Layer 2 automation events and controllers.

use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, RunRequest, SensorEvalStatus, TickStatus,
    TriggerSource, sha256_short,
};
use chrono::{TimeZone, Utc};

#[test]
fn test_schedule_ticked_event_idempotency_key() {
    let scheduled_for = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: "01HQ123SCHEDXYZ".into(), // ULID format per P0-3
            scheduled_for,
            tick_id: "01HQ123SCHEDXYZ:1736935200".into(),
            definition_version: "def_01HQ123".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            status: TickStatus::Triggered,
            run_key: Some("sched:01HQ123SCHEDXYZ:1736935200".into()),
            request_fingerprint: Some("abc123".into()),
        },
    );

    assert_eq!(event.event_type, "ScheduleTicked");
    // Idempotency key uses tick_id
    assert_eq!(
        event.idempotency_key,
        "sched_tick:01HQ123SCHEDXYZ:1736935200"
    );
}

#[test]
fn test_schedule_ticked_skipped_has_no_run_key() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: "01HQ123SCHEDXYZ".into(),
            scheduled_for: Utc::now(),
            tick_id: "01HQ123SCHEDXYZ:1736935200".into(),
            definition_version: "def_01HQ123".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            status: TickStatus::Skipped {
                reason: "paused".into(),
            },
            run_key: None,
            request_fingerprint: None,
        },
    );

    assert!(matches!(
        &event.data,
        OrchestrationEventData::ScheduleTicked { run_key: None, .. }
    ));
}

#[test]
fn test_schedule_ticked_uses_stable_ulid() {
    let scheduled_for = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: "01HQ123SCHEDXYZ".into(), // ULID, not name
            scheduled_for,
            tick_id: "01HQ123SCHEDXYZ:1736935200".into(),
            definition_version: "def_01HQ123".into(),
            asset_selection: vec!["asset_b".into()],
            partition_selection: None,
            status: TickStatus::Triggered,
            run_key: Some("sched:01HQ123SCHEDXYZ:1736935200".into()),
            request_fingerprint: Some("fp1".into()),
        },
    );

    // Verify schedule_id is ULID format (not human-readable name)
    if let OrchestrationEventData::ScheduleTicked { schedule_id, .. } = &event.data {
        // Should be alphanumeric ULID-style, not "daily-etl"
        assert!(
            schedule_id.chars().all(|c| c.is_alphanumeric()),
            "schedule_id should be ULID format"
        );
    }
}

#[test]
fn test_schedule_ticked_event_no_run_id() {
    // ScheduleTicked doesn't have a direct run_id - runs are created separately
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: "01HQ123SCHEDXYZ".into(),
            scheduled_for: Utc::now(),
            tick_id: "01HQ123SCHEDXYZ:1736935200".into(),
            definition_version: "def_01HQ123".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            status: TickStatus::Triggered,
            run_key: Some("sched:01HQ123SCHEDXYZ:1736935200".into()),
            request_fingerprint: Some("abc123".into()),
        },
    );

    // ScheduleTicked has run_key but no run_id (run created via RunRequested)
    assert!(event.data.run_id().is_none());
    assert!(event.correlation_id.is_none()); // No run_id means no correlation_id
}

// ============================================================================
// Sensor Event Tests
// ============================================================================

#[test]
fn test_sensor_evaluated_push_idempotency() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: "01HQ123SENSORXY".into(), // ULID format
            eval_id: "eval_01HQ123".into(),
            cursor_before: None,
            cursor_after: Some("file://bucket/path/file.parquet".into()),
            expected_state_version: None,
            trigger_source: TriggerSource::Push {
                message_id: "msg_abc123".into(),
            },
            run_requests: vec![RunRequest {
                run_key: "sensor:01HQ123SENSORXY:msg:msg_abc123".into(),
                request_fingerprint: "fp1".into(),
                asset_selection: vec!["raw.events".into()],
                partition_selection: None,
            }],
            status: SensorEvalStatus::Triggered,
        },
    );

    // Push sensor idempotency based on message_id
    assert!(event.idempotency_key.contains("msg:msg_abc123"));
    assert_eq!(
        event.idempotency_key,
        "sensor_eval:01HQ123SENSORXY:msg:msg_abc123"
    );
}

#[test]
fn test_sensor_evaluated_poll_idempotency_uses_cursor_before() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: "01HQ123POLLSENS".into(),
            eval_id: "eval_01HQ456".into(),
            cursor_before: Some("cursor_v1".into()),
            cursor_after: Some("cursor_v2".into()),
            expected_state_version: Some(7),
            trigger_source: TriggerSource::Poll { poll_epoch: 1736935200 },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
    );

    // Poll sensor idempotency based on cursor_before (input), not cursor_after
    let cursor_hash = sha256_short("cursor_v1");
    assert!(event.idempotency_key.contains(&cursor_hash));
    assert!(event.idempotency_key.contains("poll:1736935200"));
}

#[test]
fn test_sensor_evaluated_poll_with_cas_version() {
    // Poll sensors include expected_state_version for CAS check in fold
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: "01HQ123POLLSENS".into(),
            eval_id: "eval_01HQ789".into(),
            cursor_before: Some("cursor_v3".into()),
            cursor_after: Some("cursor_v4".into()),
            expected_state_version: Some(42), // CAS field
            trigger_source: TriggerSource::Poll { poll_epoch: 1736935300 },
            run_requests: vec![RunRequest {
                run_key: "sensor:01HQ123POLLSENS:poll:record_123".into(),
                request_fingerprint: "fp_poll".into(),
                asset_selection: vec!["derived.metrics".into()],
                partition_selection: None,
            }],
            status: SensorEvalStatus::Triggered,
        },
    );

    if let OrchestrationEventData::SensorEvaluated {
        expected_state_version,
        ..
    } = &event.data
    {
        assert_eq!(*expected_state_version, Some(42));
    }
}

#[test]
fn test_sensor_evaluated_no_run_id() {
    // SensorEvaluated doesn't have a direct run_id - runs are created separately
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: "01HQ123SENSORXY".into(),
            eval_id: "eval_01HQ123".into(),
            cursor_before: None,
            cursor_after: None,
            expected_state_version: None,
            trigger_source: TriggerSource::Push {
                message_id: "msg_xyz".into(),
            },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
    );

    assert!(event.data.run_id().is_none());
}
