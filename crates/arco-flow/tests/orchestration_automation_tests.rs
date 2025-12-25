//! Tests for Layer 2 automation events and controllers.

use arco_flow::orchestration::events::{
    BackfillState, OrchestrationEvent, OrchestrationEventData, PartitionSelector, RunRequest,
    SensorEvalStatus, TickStatus, TriggerSource, sha256_hex,
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
            trigger_source: TriggerSource::Poll {
                poll_epoch: 1736935200,
            },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
    );

    // Poll sensor idempotency based on cursor_before (input), not cursor_after
    let cursor_hash = sha256_hex("cursor_v1");
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
            trigger_source: TriggerSource::Poll {
                poll_epoch: 1736935300,
            },
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

// ============================================================================
// Backfill Event Tests
// ============================================================================

#[test]
fn test_backfill_chunk_planned_idempotency() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::BackfillChunkPlanned {
            backfill_id: "bf_01HQ123".into(),
            chunk_id: "bf_01HQ123:0".into(),
            chunk_index: 0,
            partition_keys: vec!["2025-01-01".into(), "2025-01-02".into()],
            run_key: "backfill:bf_01HQ123:chunk:0".into(),
            request_fingerprint: "fp_chunk0".into(),
        },
    );

    assert_eq!(event.event_type, "BackfillChunkPlanned");
    assert!(
        event
            .idempotency_key
            .contains("backfill_chunk:bf_01HQ123:0")
    );
}

#[test]
fn test_backfill_state_changed_uses_state_version() {
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::BackfillStateChanged {
            backfill_id: "bf_01HQ123".into(),
            from_state: BackfillState::Running,
            to_state: BackfillState::Paused,
            state_version: 3,
            changed_by: Some("user@example.com".into()),
        },
    );

    // Idempotency uses state_version (monotonic)
    assert!(
        event
            .idempotency_key
            .contains("backfill_state:bf_01HQ123:3")
    );
}

#[test]
fn test_backfill_created_uses_compact_selector() {
    // Per P0-6, BackfillCreated uses PartitionSelector, not full partition list
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::BackfillCreated {
            backfill_id: "bf_01HQ123".into(),
            client_request_id: "req_abc123".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selector: PartitionSelector::Range {
                start: "2025-01-01".into(),
                end: "2025-12-31".into(),
            },
            total_partitions: 365,
            chunk_size: 10,
            max_concurrent_runs: 5,
            parent_backfill_id: None,
        },
    );

    assert_eq!(event.event_type, "BackfillCreated");
    // Idempotency uses client_request_id
    assert!(event.idempotency_key.contains("backfill_create:req_abc123"));
}

#[test]
fn test_backfill_events_no_run_id() {
    // Backfill events don't have direct run_id - runs are created via chunks
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::BackfillCreated {
            backfill_id: "bf_01HQ123".into(),
            client_request_id: "req_xyz".into(),
            asset_selection: vec!["test".into()],
            partition_selector: PartitionSelector::Explicit {
                partition_keys: vec!["p1".into()],
            },
            total_partitions: 1,
            chunk_size: 1,
            max_concurrent_runs: 1,
            parent_backfill_id: None,
        },
    );

    assert!(event.data.run_id().is_none());
}

// ============================================================================
// RunRequested Event Tests
// ============================================================================

#[test]
fn test_run_requested_stable_run_key_fingerprint_in_payload() {
    use arco_flow::orchestration::events::SourceRef;
    use std::collections::HashMap;

    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: "sched:01HQ123SCHEDXYZ:1736935200".into(),
            request_fingerprint: "fingerprint_v1".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: "01HQ123SCHEDXYZ".into(),
                tick_id: "01HQ123SCHEDXYZ:1736935200".into(),
            },
            labels: HashMap::new(),
        },
    );

    // run_key is stable (no fingerprint in it)
    assert!(!event.idempotency_key.contains("fingerprint"));

    // But fingerprint hash IS in the idempotency key
    let fp_hash = sha256_hex("fingerprint_v1");
    assert!(event.idempotency_key.contains(&fp_hash));
}

#[test]
fn test_run_requested_idempotency_includes_fingerprint_hash() {
    use arco_flow::orchestration::events::SourceRef;
    use std::collections::HashMap;

    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: "sched:01HQ123:1736935200".into(),
            request_fingerprint: "fingerprint_v1".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: "01HQ123".into(),
                tick_id: "01HQ123:1736935200".into(),
            },
            labels: HashMap::new(),
        },
    );

    let fp_hash = sha256_hex("fingerprint_v1");
    assert!(
        event
            .idempotency_key
            .starts_with("runreq:sched:01HQ123:1736935200:")
    );
    assert!(event.idempotency_key.contains(&fp_hash));
}

#[test]
fn test_run_requested_different_fingerprints_different_idempotency() {
    use arco_flow::orchestration::events::SourceRef;
    use std::collections::HashMap;

    let event1 = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: "sched:01HQ123:1736935200".into(),
            request_fingerprint: "fingerprint_v1".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: "01HQ123".into(),
                tick_id: "01HQ123:1736935200".into(),
            },
            labels: HashMap::new(),
        },
    );

    let event2 = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: "sched:01HQ123:1736935200".into(), // Same run_key
            request_fingerprint: "fingerprint_v2".into(), // Different fingerprint
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: "01HQ123".into(),
                tick_id: "01HQ123:1736935200".into(),
            },
            labels: HashMap::new(),
        },
    );

    // Same run_key but different fingerprints = different idempotency keys
    assert_ne!(event1.idempotency_key, event2.idempotency_key);

    // Both should start with the same prefix (run_key)
    assert!(
        event1
            .idempotency_key
            .starts_with("runreq:sched:01HQ123:1736935200:")
    );
    assert!(
        event2
            .idempotency_key
            .starts_with("runreq:sched:01HQ123:1736935200:")
    );
}

#[test]
fn test_run_requested_no_run_id() {
    use arco_flow::orchestration::events::SourceRef;
    use std::collections::HashMap;

    // RunRequested doesn't have a direct run_id - it's computed from run_key at fold time
    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: "sensor:01HQ123:msg:msg_abc".into(),
            request_fingerprint: "fp1".into(),
            asset_selection: vec!["raw.events".into()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Sensor {
                sensor_id: "01HQ123".into(),
                eval_id: "eval_xyz".into(),
            },
            labels: HashMap::new(),
        },
    );

    assert!(event.data.run_id().is_none());
    assert_eq!(event.event_type, "RunRequested");
}

#[test]
fn test_run_requested_with_backfill_source() {
    use arco_flow::orchestration::events::SourceRef;
    use std::collections::HashMap;

    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: "backfill:bf_01HQ123:chunk:5".into(),
            request_fingerprint: "fp_chunk5".into(),
            asset_selection: vec!["analytics.summary".into()],
            partition_selection: Some(vec!["2025-01-05".into(), "2025-01-06".into()]),
            trigger_source_ref: SourceRef::Backfill {
                backfill_id: "bf_01HQ123".into(),
                chunk_id: "bf_01HQ123:5".into(),
            },
            labels: HashMap::new(),
        },
    );

    assert_eq!(event.event_type, "RunRequested");
    assert!(
        event
            .idempotency_key
            .starts_with("runreq:backfill:bf_01HQ123:chunk:5:")
    );
}

#[test]
fn test_run_requested_with_manual_source() {
    use arco_flow::orchestration::events::SourceRef;
    use std::collections::HashMap;

    let mut labels = HashMap::new();
    labels.insert("priority".into(), "high".into());
    labels.insert("reason".into(), "hotfix".into());

    let event = OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: "manual:user123:req_abc123".into(),
            request_fingerprint: "fp_manual".into(),
            asset_selection: vec!["critical.table".into()],
            partition_selection: Some(vec!["2025-01-15".into()]),
            trigger_source_ref: SourceRef::Manual {
                user_id: "user123".into(),
                request_id: "req_abc123".into(),
            },
            labels,
        },
    );

    assert_eq!(event.event_type, "RunRequested");
    assert!(
        event
            .idempotency_key
            .starts_with("runreq:manual:user123:req_abc123:")
    );
}
