//! Schedule semantics end-to-end (hermetic, CI-gated).
//!
//! This suite proves parity-06 schedule semantics across:
//! persisted schedule definitions -> controller tick evaluation -> RunRequested/run_key ->
//! durable tick history and run correlation.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{Duration, TimeZone, Utc};

use arco_core::{MemoryBackend, ScopedStorage, WritePrecondition};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::controllers::schedule::ScheduleController;
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, SourceRef, TickStatus,
};
use arco_flow::orchestration::ids::run_id_from_run_key;

fn orchestration_event_path(date: &str, event_id: &str) -> String {
    format!("ledger/orchestration/{date}/{event_id}.json")
}

#[tokio::test]
async fn schedule_tick_history_survives_compactor_reload_and_is_idempotent()
-> arco_flow::error::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let schedule_id = "daily-etl";
    let scheduled_for = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let epoch = scheduled_for.timestamp();
    let tick_id = format!("{schedule_id}:{epoch}");
    let run_key = format!("sched:{schedule_id}:{epoch}");
    let fingerprint = "fp_01".to_string();

    let mut tick_event = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: schedule_id.to_string(),
            scheduled_for,
            tick_id: tick_id.clone(),
            definition_version: "def_v1".to_string(),
            asset_selection: vec!["analytics.summary".to_string()],
            partition_selection: None,
            status: TickStatus::Triggered,
            run_key: Some(run_key.clone()),
            request_fingerprint: Some(fingerprint.clone()),
        },
        // Keep emitted_at deterministic.
        scheduled_for,
    );
    tick_event.event_id = "evt_01_sched_tick".to_string();

    let mut run_req = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: run_key.clone(),
            request_fingerprint: fingerprint.clone(),
            asset_selection: vec!["analytics.summary".to_string()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: schedule_id.to_string(),
                tick_id: tick_id.clone(),
            },
            labels: HashMap::new(),
        },
        scheduled_for,
    );
    run_req.event_id = "evt_02_run_req".to_string();

    // Write explicit ledger files and compact in one batch.
    let path1 = orchestration_event_path("2025-01-01", &tick_event.event_id);
    storage
        .put_raw(
            &path1,
            Bytes::from(serde_json::to_string(&tick_event).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;

    let path2 = orchestration_event_path("2025-01-01", &run_req.event_id);
    storage
        .put_raw(
            &path2,
            Bytes::from(serde_json::to_string(&run_req).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;

    let compactor = MicroCompactor::new(storage.clone());
    compactor.compact_events(vec![path1, path2]).await?;

    // Reload and assert durable state.
    let (_, state) = MicroCompactor::new(storage.clone()).load_state().await?;

    let tick = state
        .schedule_ticks
        .get(tick_id.as_str())
        .expect("expected tick row to be persisted");
    assert_eq!(tick.tick_id, tick_id);
    assert_eq!(tick.schedule_id, schedule_id);
    assert_eq!(tick.run_key.as_deref(), Some(run_key.as_str()));
    assert!(tick.run_id.is_some(), "run_id should be correlated");

    // Duplicate delivery with new event IDs but same idempotency keys must be a no-op.
    let mut tick_dup = tick_event.clone();
    tick_dup.event_id = "evt_03_sched_tick_dup".to_string();
    let dup_path1 = orchestration_event_path("2025-01-01", &tick_dup.event_id);
    storage
        .put_raw(
            &dup_path1,
            Bytes::from(serde_json::to_string(&tick_dup).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;

    let mut run_req_dup = run_req.clone();
    run_req_dup.event_id = "evt_04_run_req_dup".to_string();
    let dup_path2 = orchestration_event_path("2025-01-01", &run_req_dup.event_id);
    storage
        .put_raw(
            &dup_path2,
            Bytes::from(serde_json::to_string(&run_req_dup).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;

    let compactor2 = MicroCompactor::new(storage.clone());
    compactor2
        .compact_events(vec![dup_path1, dup_path2])
        .await?;

    let (_, state2) = MicroCompactor::new(storage).load_state().await?;
    assert_eq!(state2.schedule_ticks.len(), 1, "no duplicate tick rows");

    Ok(())
}

#[tokio::test]
async fn schedule_definition_version_is_snapshotted_into_ticks() -> arco_flow::error::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let schedule_id = "daily-etl".to_string();

    // Persist a schedule definition.
    let mut def_event = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleDefinitionUpserted {
            schedule_id: schedule_id.clone(),
            cron_expression: "0 0 10 * * *".to_string(),
            timezone: "UTC".to_string(),
            catchup_window_minutes: 60 * 24,
            asset_selection: vec!["analytics.summary".to_string()],
            max_catchup_ticks: 5,
            enabled: true,
        },
        Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
    );
    def_event.event_id = "evt_01_def".to_string();

    let path = orchestration_event_path("2025-01-01", &def_event.event_id);
    storage
        .put_raw(
            &path,
            Bytes::from(serde_json::to_string(&def_event).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;

    MicroCompactor::new(storage.clone())
        .compact_events(vec![path])
        .await?;

    // Load definitions from persisted compactor state.
    let (_, state) = MicroCompactor::new(storage.clone()).load_state().await?;
    let def = state
        .schedule_definitions
        .get(schedule_id.as_str())
        .expect("expected persisted schedule definition");

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap();
    let controller = ScheduleController::new();
    let events = controller.reconcile(&[def.clone()], &[], now);

    // Controller must emit one due tick within the window.
    let tick_event = events
        .iter()
        .find_map(|e| match &e.data {
            OrchestrationEventData::ScheduleTicked { .. } => Some(e.clone()),
            _ => None,
        })
        .expect("expected ScheduleTicked event");

    let mut ledger_events = Vec::new();
    for (i, mut e) in events.iter().cloned().enumerate() {
        // Deterministic event IDs (ordering is handled by compactor sorting).
        e.event_id = format!("evt_02_tick_batch_{i:02}");
        ledger_events.push(e);
    }

    let ledger = LedgerWriter::new(storage.clone());
    let mut event_paths = Vec::new();
    for e in ledger_events {
        let p = LedgerWriter::event_path(&e);
        ledger.append(e).await?;
        event_paths.push(p);
    }

    MicroCompactor::new(storage.clone())
        .compact_events(event_paths)
        .await?;

    let OrchestrationEventData::ScheduleTicked {
        tick_id,
        scheduled_for,
        definition_version,
        status,
        run_key,
        ..
    } = &tick_event.data
    else {
        unreachable!();
    };

    // Stable identity and run_key rules per ADR-024.
    assert!(matches!(status, TickStatus::Triggered));

    let epoch = scheduled_for.timestamp();
    let expected_tick_id = format!("{schedule_id}:{epoch}");
    let expected_run_key = format!("sched:{schedule_id}:{epoch}");

    assert_eq!(tick_id.as_str(), expected_tick_id.as_str());
    assert_eq!(run_key.as_deref(), Some(expected_run_key.as_str()));

    let has_matching_run_requested = events.iter().any(|e| {
        matches!(
            &e.data,
            OrchestrationEventData::RunRequested {
                run_key,
                trigger_source_ref: SourceRef::Schedule {
                    schedule_id: source_schedule_id,
                    tick_id: source_tick_id,
                },
                ..
            } if run_key == &expected_run_key
                && source_schedule_id == &schedule_id
                && source_tick_id == &expected_tick_id
        )
    });

    assert!(
        has_matching_run_requested,
        "triggered ticks must emit a matching RunRequested in the same reconcile batch"
    );

    let (_, state2) = MicroCompactor::new(storage).load_state().await?;
    let tick_row = state2
        .schedule_ticks
        .get(tick_id.as_str())
        .expect("expected tick row");

    assert_eq!(
        tick_row.definition_version, *definition_version,
        "tick row must snapshot the definition row_version used at evaluation time"
    );

    Ok(())
}

#[tokio::test]
async fn schedule_controller_bounded_catchup_emits_only_recent_ticks()
-> arco_flow::error::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let schedule_id = "daily-etl".to_string();

    // Persist a definition with strict catchup bounds.
    let created_at = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let mut def_event = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleDefinitionUpserted {
            schedule_id: schedule_id.clone(),
            cron_expression: "0 */10 * * * *".to_string(),
            timezone: "UTC".to_string(),
            catchup_window_minutes: 60,
            asset_selection: vec!["analytics.summary".to_string()],
            max_catchup_ticks: 3,
            enabled: true,
        },
        created_at,
    );
    def_event.event_id = "evt_def_bounds".to_string();

    let path = orchestration_event_path("2025-01-01", &def_event.event_id);
    storage
        .put_raw(
            &path,
            Bytes::from(serde_json::to_string(&def_event).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    MicroCompactor::new(storage.clone())
        .compact_events(vec![path])
        .await?;

    let (_, state) = MicroCompactor::new(storage.clone()).load_state().await?;
    let def = state
        .schedule_definitions
        .get(schedule_id.as_str())
        .expect("expected persisted schedule definition");

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 35, 0).unwrap();
    let controller = ScheduleController::new();
    let events = controller.reconcile(&[def.clone()], &[], now);

    let ticks: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.data {
            OrchestrationEventData::ScheduleTicked { scheduled_for, .. } => Some(*scheduled_for),
            _ => None,
        })
        .collect();

    assert!(!ticks.is_empty(), "expected some due ticks");
    assert!(ticks.len() <= 3, "max_catchup_ticks must cap outputs");
    for scheduled_for in ticks {
        assert!(
            scheduled_for >= now - Duration::minutes(60),
            "catchup window must clamp to now - window"
        );
        assert!(scheduled_for <= now);
    }

    Ok(())
}

#[tokio::test]
async fn schedule_tick_run_correlation_survives_out_of_order_compaction()
-> arco_flow::error::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let schedule_id = "daily-etl";
    let scheduled_for = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let epoch = scheduled_for.timestamp();
    let tick_id = format!("{schedule_id}:{epoch}");
    let run_key = format!("sched:{schedule_id}:{epoch}");
    let fingerprint = "fp_01".to_string();

    let mut run_req = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: run_key.clone(),
            request_fingerprint: fingerprint.clone(),
            asset_selection: vec!["analytics.summary".to_string()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: schedule_id.to_string(),
                tick_id: tick_id.clone(),
            },
            labels: HashMap::new(),
        },
        scheduled_for,
    );
    run_req.event_id = "evt_01_run_req_out_of_order".to_string();

    let mut tick_event = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleTicked {
            schedule_id: schedule_id.to_string(),
            scheduled_for,
            tick_id: tick_id.clone(),
            definition_version: "def_v1".to_string(),
            asset_selection: vec!["analytics.summary".to_string()],
            partition_selection: None,
            status: TickStatus::Triggered,
            run_key: Some(run_key.clone()),
            request_fingerprint: Some(fingerprint.clone()),
        },
        scheduled_for,
    );
    tick_event.event_id = "evt_02_tick_out_of_order".to_string();

    let path1 = orchestration_event_path("2025-01-01", &run_req.event_id);
    storage
        .put_raw(
            &path1,
            Bytes::from(serde_json::to_string(&run_req).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    MicroCompactor::new(storage.clone())
        .compact_events(vec![path1])
        .await?;

    let path2 = orchestration_event_path("2025-01-01", &tick_event.event_id);
    storage
        .put_raw(
            &path2,
            Bytes::from(serde_json::to_string(&tick_event).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    MicroCompactor::new(storage.clone())
        .compact_events(vec![path2])
        .await?;

    let (_, state) = MicroCompactor::new(storage).load_state().await?;
    let tick = state
        .schedule_ticks
        .get(tick_id.as_str())
        .expect("expected tick row");

    let expected_run_id = run_id_from_run_key("tenant-abc", "workspace-prod", &run_key, &[]);
    assert_eq!(tick.run_key.as_deref(), Some(run_key.as_str()));
    assert_eq!(tick.run_id.as_deref(), Some(expected_run_id.as_str()));

    Ok(())
}

#[tokio::test]
async fn disabled_schedule_emits_skipped_tick_without_run_request() -> arco_flow::error::Result<()>
{
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let schedule_id = "daily-etl".to_string();

    let created_at = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let mut def_event = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleDefinitionUpserted {
            schedule_id: schedule_id.clone(),
            cron_expression: "0 0 10 * * *".to_string(),
            timezone: "UTC".to_string(),
            catchup_window_minutes: 60 * 24,
            asset_selection: vec!["analytics.summary".to_string()],
            max_catchup_ticks: 1,
            enabled: false,
        },
        created_at,
    );
    def_event.event_id = "evt_def_disabled".to_string();

    let def_path = orchestration_event_path("2025-01-01", &def_event.event_id);
    storage
        .put_raw(
            &def_path,
            Bytes::from(serde_json::to_string(&def_event).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    MicroCompactor::new(storage.clone())
        .compact_events(vec![def_path])
        .await?;

    let (_, state) = MicroCompactor::new(storage.clone()).load_state().await?;
    let def = state
        .schedule_definitions
        .get(schedule_id.as_str())
        .expect("expected persisted schedule definition");

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap();
    let controller = ScheduleController::new();
    let events = controller.reconcile(&[def.clone()], &[], now);

    assert!(
        !events
            .iter()
            .any(|e| matches!(e.data, OrchestrationEventData::RunRequested { .. })),
        "disabled schedule must not emit RunRequested"
    );

    let tick_event = events
        .iter()
        .find(|e| matches!(e.data, OrchestrationEventData::ScheduleTicked { .. }))
        .expect("expected ScheduleTicked event")
        .clone();

    let OrchestrationEventData::ScheduleTicked {
        tick_id,
        status,
        run_key,
        ..
    } = &tick_event.data
    else {
        unreachable!();
    };

    let tick_id = tick_id.clone();

    assert!(matches!(status, TickStatus::Skipped { .. }));
    assert!(run_key.is_none(), "skipped ticks have no run_key");

    let ledger = LedgerWriter::new(storage.clone());
    let p = LedgerWriter::event_path(&tick_event);
    ledger.append(tick_event).await?;

    MicroCompactor::new(storage.clone())
        .compact_events(vec![p])
        .await?;

    let (_, state2) = MicroCompactor::new(storage).load_state().await?;
    let tick = state2
        .schedule_ticks
        .get(tick_id.as_str())
        .expect("expected tick row");

    assert!(tick.run_key.is_none());
    assert!(tick.run_id.is_none());

    Ok(())
}

#[tokio::test]
async fn invalid_cron_emits_failed_tick_with_deterministic_tick_id() -> arco_flow::error::Result<()>
{
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let schedule_id = "daily-etl".to_string();

    let created_at = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let mut def_event = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::ScheduleDefinitionUpserted {
            schedule_id: schedule_id.clone(),
            cron_expression: "not-a-cron".to_string(),
            timezone: "UTC".to_string(),
            catchup_window_minutes: 60,
            asset_selection: vec!["analytics.summary".to_string()],
            max_catchup_ticks: 3,
            enabled: true,
        },
        created_at,
    );
    def_event.event_id = "evt_def_invalid_cron".to_string();

    let def_path = orchestration_event_path("2025-01-01", &def_event.event_id);
    storage
        .put_raw(
            &def_path,
            Bytes::from(serde_json::to_string(&def_event).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    MicroCompactor::new(storage.clone())
        .compact_events(vec![def_path])
        .await?;

    let (_, state) = MicroCompactor::new(storage.clone()).load_state().await?;
    let def = state
        .schedule_definitions
        .get(schedule_id.as_str())
        .expect("expected persisted schedule definition");

    let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap();
    let controller = ScheduleController::new();
    let events = controller.reconcile(&[def.clone()], &[], now);

    assert!(
        !events
            .iter()
            .any(|e| matches!(e.data, OrchestrationEventData::RunRequested { .. })),
        "invalid cron must not emit RunRequested"
    );

    assert_eq!(
        events.len(),
        1,
        "invalid cron should emit exactly one failed ScheduleTicked"
    );

    let tick_event = events
        .into_iter()
        .find(|e| matches!(e.data, OrchestrationEventData::ScheduleTicked { .. }))
        .expect("expected ScheduleTicked event");

    let OrchestrationEventData::ScheduleTicked {
        tick_id,
        scheduled_for,
        status,
        run_key,
        ..
    } = &tick_event.data
    else {
        unreachable!();
    };

    assert_eq!(scheduled_for, &now, "error ticks use evaluation time");
    assert!(matches!(status, TickStatus::Failed { .. }));
    assert!(run_key.is_none(), "failed ticks have no run_key");

    let expected_tick_id = format!("{schedule_id}:{}", now.timestamp());
    assert_eq!(tick_id.as_str(), expected_tick_id.as_str());

    let ledger = LedgerWriter::new(storage.clone());
    let p = LedgerWriter::event_path(&tick_event);
    ledger.append(tick_event).await?;

    MicroCompactor::new(storage.clone())
        .compact_events(vec![p])
        .await?;

    let (_, state2) = MicroCompactor::new(storage).load_state().await?;
    let tick = state2
        .schedule_ticks
        .get(expected_tick_id.as_str())
        .expect("expected persisted tick row");

    assert!(matches!(tick.status, TickStatus::Failed { .. }));
    assert!(tick.run_key.is_none());
    assert!(tick.run_id.is_none());

    Ok(())
}
