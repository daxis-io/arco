//! Tests for orchestration event envelope and event types.

use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData, TriggerInfo};
use chrono::Utc;

#[test]
fn test_event_envelope_serialization() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunTriggered {
            run_id: "01HQXYZ123RUN".into(),
            plan_id: "01HQXYZ123PLN".into(),
            trigger: TriggerInfo::Manual {
                user_id: "user@example.com".into(),
            },
            root_assets: vec!["analytics.daily_summary".into()],
            run_key: None,
        },
    );

    let json = serde_json::to_string(&event).unwrap();
    let parsed: OrchestrationEvent = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.event_type, "RunTriggered");
    assert!(parsed.idempotency_key.starts_with("run:"));
    assert_eq!(parsed.tenant_id, "tenant-abc");
    assert_eq!(parsed.workspace_id, "workspace-prod");
}

#[test]
fn test_run_triggered_idempotency_key() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunTriggered {
            run_id: "01HQXYZ123RUN".into(),
            plan_id: "01HQXYZ123PLN".into(),
            trigger: TriggerInfo::Cron {
                schedule_id: "daily-etl".into(),
            },
            root_assets: vec!["analytics.daily_summary".into()],
            run_key: Some("daily-etl:2025-01-15".into()),
        },
    );

    // Idempotency key should include run_key for deduplication
    assert!(event.idempotency_key.contains("daily-etl:2025-01-15"));
}

#[test]
fn test_task_finished_with_attempt_id() {
    let attempt_id = "01HQXYZ456ATT".to_string();

    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::TaskFinished {
            run_id: "01HQXYZ123RUN".into(),
            task_key: "extract".into(),
            attempt: 1,
            attempt_id: attempt_id.clone(),
            outcome: arco_flow::orchestration::events::TaskOutcome::Succeeded,
            materialization_id: Some("01HQXYZ789MAT".into()),
            error_message: None,
        },
    );

    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains(&attempt_id));

    // Idempotency key includes attempt for uniqueness
    assert!(event.idempotency_key.contains("extract:1"));
}

#[test]
fn test_task_started_includes_attempt_id() {
    let attempt_id = "01HQXYZ456ATT".to_string();

    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::TaskStarted {
            run_id: "01HQXYZ123RUN".into(),
            task_key: "extract".into(),
            attempt: 1,
            attempt_id: attempt_id.clone(),
            worker_id: "worker-01".into(),
        },
    );

    // attempt_id is included in the event
    if let OrchestrationEventData::TaskStarted {
        attempt_id: aid, ..
    } = &event.data
    {
        assert_eq!(aid, &attempt_id);
    } else {
        panic!("Expected TaskStarted event");
    }
}

#[test]
fn test_dispatch_requested_event() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::DispatchRequested {
            run_id: "01HQXYZ123RUN".into(),
            task_key: "extract".into(),
            attempt: 1,
            attempt_id: "01HQXYZ456ATT".into(),
            dispatch_id: "dispatch:01HQXYZ123RUN:extract:1".into(),
        },
    );

    assert_eq!(event.event_type, "DispatchRequested");
}

#[test]
fn test_timer_requested_event() {
    let fire_at = Utc::now() + chrono::Duration::seconds(60);

    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::TimerRequested {
            timer_id: "timer:retry:01HQXYZ123RUN:extract:1:1705340400".into(),
            timer_type: arco_flow::orchestration::events::TimerType::Retry,
            run_id: Some("01HQXYZ123RUN".into()),
            task_key: Some("extract".into()),
            attempt: Some(1),
            fire_at,
        },
    );

    assert_eq!(event.event_type, "TimerRequested");
}

#[test]
fn test_event_version_is_set() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunTriggered {
            run_id: "01HQXYZ123RUN".into(),
            plan_id: "01HQXYZ123PLN".into(),
            trigger: TriggerInfo::Manual {
                user_id: "user@example.com".into(),
            },
            root_assets: vec![],
            run_key: None,
        },
    );

    assert_eq!(event.event_version, 1);
}
