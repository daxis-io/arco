//! Tests for Layer 2 automation events and controllers.

use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TickStatus,
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
