//! Parity gate suite M2: schedules/sensors/backfills workflows.
//!
//! Focus: controller emission semantics (atomic batches), bounded catchup, CAS-friendly
//! poll sensors, and backfill chunk planning invariants.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};

use arco_flow::orchestration::compactor::FoldState;
use arco_flow::orchestration::compactor::fold::{
    BackfillChunkRow, BackfillRow, RunRow, ScheduleDefinitionRow, ScheduleStateRow, SensorStateRow,
};
use arco_flow::orchestration::compactor::manifest::Watermarks;
use arco_flow::orchestration::controllers::backfill::{BackfillController, PartitionResolver};
use arco_flow::orchestration::controllers::schedule::ScheduleController;
use arco_flow::orchestration::controllers::sensor::PollSensorController;
use arco_flow::orchestration::events::{
    BackfillState, ChunkState, OrchestrationEvent, OrchestrationEventData, SensorEvalStatus,
    SensorStatus, SourceRef, TickStatus, TriggerSource,
};

#[derive(Debug)]
struct StaticPartitionResolver {
    partitions: Vec<String>,
}

impl StaticPartitionResolver {
    fn new(partitions: Vec<String>) -> Self {
        Self { partitions }
    }
}

impl PartitionResolver for StaticPartitionResolver {
    fn count_range(&self, _asset_key: &str, _start: &str, _end: &str) -> u32 {
        u32::try_from(self.partitions.len()).unwrap_or(u32::MAX)
    }

    fn list_range_chunk(
        &self,
        _asset_key: &str,
        _start: &str,
        _end: &str,
        offset: u32,
        limit: u32,
    ) -> Vec<String> {
        let offset = offset as usize;
        let limit = limit as usize;
        self.partitions
            .iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect()
    }
}

fn schedule_definition(
    enabled: bool,
    max_catchup_ticks: u32,
    catchup_window_minutes: u32,
) -> ScheduleDefinitionRow {
    ScheduleDefinitionRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        schedule_id: "daily-etl".into(),
        cron_expression: "*/1 * * * *".into(),
        timezone: "UTC".into(),
        catchup_window_minutes,
        asset_selection: vec!["analytics.summary".into()],
        max_catchup_ticks,
        enabled,
        created_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        row_version: "01HQDEF".into(),
    }
}

fn schedule_state(last_scheduled_for: chrono::DateTime<Utc>) -> ScheduleStateRow {
    ScheduleStateRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        schedule_id: "daily-etl".into(),
        last_scheduled_for: Some(last_scheduled_for),
        last_tick_id: None,
        last_run_key: None,
        row_version: "01HQSTATE".into(),
    }
}

#[test]
fn parity_m2_schedule_reconcile_emits_tick_and_run_requested_in_same_batch() {
    let controller = ScheduleController::new();

    let def = schedule_definition(true, 3, 0);
    let last = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2025, 1, 1, 0, 1, 30).unwrap();

    let events = controller.reconcile(&[def.clone()], &[schedule_state(last)], now);

    let tick = events
        .iter()
        .find_map(|e| match &e.data {
            OrchestrationEventData::ScheduleTicked { .. } => Some(e),
            _ => None,
        })
        .expect("expected ScheduleTicked");

    let run_req = events
        .iter()
        .find_map(|e| match &e.data {
            OrchestrationEventData::RunRequested { .. } => Some(e),
            _ => None,
        })
        .expect("expected RunRequested");

    // Atomic batch: both events returned together.
    let tick_count = events
        .iter()
        .filter(|e| matches!(&e.data, OrchestrationEventData::ScheduleTicked { .. }))
        .count();
    let run_req_count = events
        .iter()
        .filter(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }))
        .count();

    assert_eq!(tick_count, 1);
    assert_eq!(run_req_count, 1);

    // RunRequested must point back to the tick that caused it.
    let OrchestrationEventData::ScheduleTicked {
        tick_id,
        run_key,
        request_fingerprint,
        ..
    } = &tick.data
    else {
        unreachable!();
    };
    let OrchestrationEventData::RunRequested {
        run_key: rrk,
        request_fingerprint: rfp,
        trigger_source_ref,
        ..
    } = &run_req.data
    else {
        unreachable!();
    };

    assert_eq!(
        rrk,
        run_key
            .as_ref()
            .expect("triggered tick should have run_key")
    );
    assert_eq!(
        rfp,
        request_fingerprint
            .as_ref()
            .expect("tick should have fingerprint")
    );

    assert!(matches!(
        trigger_source_ref,
        SourceRef::Schedule { schedule_id, tick_id: ref_tid } if schedule_id == &def.schedule_id && ref_tid == tick_id
    ));
}

#[test]
fn parity_m2_schedule_reconcile_is_bounded_by_max_catchup_ticks() {
    let controller = ScheduleController::new();

    // With no prior state and a 60 minute window, the controller may have many due ticks.
    // It must cap outputs to `max_catchup_ticks`.
    let def = schedule_definition(true, 2, 60);
    let now = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 30).unwrap();

    let events = controller.reconcile(&[def], &[], now);

    let tick_count = events
        .iter()
        .filter(|e| matches!(&e.data, OrchestrationEventData::ScheduleTicked { .. }))
        .count();
    let run_req_count = events
        .iter()
        .filter(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }))
        .count();

    assert_eq!(tick_count, 2);
    assert_eq!(run_req_count, 2);
}

#[test]
fn parity_m2_schedule_disabled_emits_history_but_does_not_request_runs() {
    let controller = ScheduleController::new();

    let def = schedule_definition(false, 1, 0);
    let last = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
    let now = Utc.with_ymd_and_hms(2025, 1, 1, 0, 1, 30).unwrap();

    let events = controller.reconcile(&[def], &[schedule_state(last)], now);

    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }))
            .count(),
        0
    );

    let tick = events
        .iter()
        .find(|e| matches!(&e.data, OrchestrationEventData::ScheduleTicked { .. }))
        .expect("expected ScheduleTicked");

    let OrchestrationEventData::ScheduleTicked {
        status, run_key, ..
    } = &tick.data
    else {
        unreachable!();
    };
    assert!(matches!(status, TickStatus::Skipped { .. }));
    assert!(run_key.is_none());
}

#[test]
fn parity_m2_poll_sensor_should_evaluate_enforces_min_interval() {
    let controller = PollSensorController::with_min_interval(Duration::seconds(30));

    let base_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();

    let state = SensorStateRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        sensor_id: "01HQ456POLLSENSOR".into(),
        cursor: Some("cursor_v1".into()),
        last_evaluation_at: Some(base_time),
        last_eval_id: Some("eval_01".into()),
        status: SensorStatus::Active,
        state_version: 7,
        row_version: "01HQROW".into(),
    };

    assert!(!controller.should_evaluate(&state, base_time + Duration::seconds(10)));
    assert!(controller.should_evaluate(&state, base_time + Duration::seconds(31)));
}

#[test]
fn parity_m2_backfill_plan_chunk_emits_chunk_and_run_requested_with_partition_selection() {
    let resolver = Arc::new(StaticPartitionResolver::new(vec![
        "2025-01-01".into(),
        "2025-01-02".into(),
    ]));
    let controller = BackfillController::new(resolver);

    let partition_keys = vec!["2025-01-01".to_string(), "2025-01-02".to_string()];
    let asset_selection = vec!["analytics.daily".to_string()];

    let events = controller.plan_chunk(
        "bf_01HQ123",
        0,
        &partition_keys,
        &asset_selection,
        "tenant-abc",
        "workspace-prod",
    );

    let planned_count = events
        .iter()
        .filter(|e| matches!(&e.data, OrchestrationEventData::BackfillChunkPlanned { .. }))
        .count();
    let run_req_count = events
        .iter()
        .filter(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }))
        .count();

    assert_eq!(planned_count, 1);
    assert_eq!(run_req_count, 1);

    let planned = events
        .iter()
        .find(|e| matches!(&e.data, OrchestrationEventData::BackfillChunkPlanned { .. }))
        .expect("expected BackfillChunkPlanned");
    let run_req = events
        .iter()
        .find(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }))
        .expect("expected RunRequested");

    let OrchestrationEventData::BackfillChunkPlanned {
        run_key, chunk_id, ..
    } = &planned.data
    else {
        unreachable!();
    };
    let OrchestrationEventData::RunRequested {
        run_key: rrk,
        partition_selection,
        trigger_source_ref,
        ..
    } = &run_req.data
    else {
        unreachable!();
    };

    assert_eq!(rrk, run_key);
    assert_eq!(partition_selection.as_ref(), Some(&partition_keys));
    assert!(matches!(
        trigger_source_ref,
        SourceRef::Backfill { backfill_id, chunk_id: ref_chunk_id }
            if backfill_id == "bf_01HQ123" && ref_chunk_id == chunk_id
    ));
}

#[test]
fn parity_m2_backfill_reconcile_returns_empty_when_compaction_watermarks_are_stale() {
    let resolver = Arc::new(StaticPartitionResolver::new(vec![
        "2025-01-01".into(),
        "2025-01-02".into(),
        "2025-01-03".into(),
    ]));

    let controller = BackfillController::with_defaults(
        resolver,
        2,                    // default_chunk_size
        1,                    // default_max_concurrent
        Duration::seconds(1), // max_compaction_lag (tight to force stale)
    );

    let watermarks = Watermarks {
        events_processed_through: None,
        last_processed_file: None,
        last_processed_at: Utc::now() - Duration::seconds(120),
    };

    let mut backfills = HashMap::new();
    backfills.insert(
        "bf_01HQ123".to_string(),
        BackfillRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            backfill_id: "bf_01HQ123".into(),
            asset_selection: vec!["analytics.daily".into()],
            partition_selector: arco_flow::orchestration::events::PartitionSelector::Range {
                start: "2025-01-01".into(),
                end: "2025-01-03".into(),
            },
            chunk_size: 2,
            max_concurrent_runs: 1,
            state: BackfillState::Running,
            state_version: 1,
            total_partitions: 3,
            planned_chunks: 0,
            completed_chunks: 0,
            failed_chunks: 0,
            parent_backfill_id: None,
            created_at: Utc::now(),
            row_version: "01HQBF".into(),
        },
    );

    let events = controller.reconcile(
        &watermarks,
        &backfills,
        &HashMap::<String, BackfillChunkRow>::new(),
        &HashMap::<String, RunRow>::new(),
    );

    assert!(events.is_empty());
}

fn poll_sensor_evaluated_event(
    sensor_id: &str,
    cursor_before: Option<&str>,
    cursor_after: Option<&str>,
    expected_state_version: Option<u32>,
) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: sensor_id.to_string(),
            eval_id: format!("eval_{sensor_id}"),
            cursor_before: cursor_before.map(ToString::to_string),
            cursor_after: cursor_after.map(ToString::to_string),
            expected_state_version,
            trigger_source: TriggerSource::Poll {
                poll_epoch: 1736935200,
            },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
    )
}

#[test]
fn parity_m2_poll_sensor_cursor_cas_rejects_stale_expected_version() {
    let mut state = FoldState::new();

    state.sensor_state.insert(
        "01HQ456POLLSENSOR".into(),
        SensorStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            sensor_id: "01HQ456POLLSENSOR".into(),
            cursor: Some("cursor_v1".into()),
            last_evaluation_at: None,
            last_eval_id: None,
            status: SensorStatus::Active,
            state_version: 2,
            row_version: String::new(),
        },
    );

    let event = poll_sensor_evaluated_event(
        "01HQ456POLLSENSOR",
        Some("cursor_v1"),
        Some("cursor_v2"),
        Some(1),
    );

    state.fold_event(&event);

    let sensor_state = state
        .sensor_state
        .get("01HQ456POLLSENSOR")
        .expect("sensor state should exist");

    assert_eq!(sensor_state.cursor.as_deref(), Some("cursor_v1"));
    assert_eq!(sensor_state.state_version, 2);

    let eval_id = match &event.data {
        OrchestrationEventData::SensorEvaluated { eval_id, .. } => eval_id,
        _ => unreachable!("expected SensorEvaluated"),
    };
    let eval_row = state
        .sensor_evals
        .get(eval_id)
        .expect("sensor eval row should exist");
    assert!(matches!(
        eval_row.status,
        SensorEvalStatus::SkippedStaleCursor
    ));
}

#[test]
fn parity_m2_poll_sensor_cursor_cas_accepts_matching_expected_version() {
    let mut state = FoldState::new();

    state.sensor_state.insert(
        "01HQ456POLLSENSOR".into(),
        SensorStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            sensor_id: "01HQ456POLLSENSOR".into(),
            cursor: Some("cursor_v1".into()),
            last_evaluation_at: None,
            last_eval_id: None,
            status: SensorStatus::Active,
            state_version: 2,
            row_version: String::new(),
        },
    );

    let event = poll_sensor_evaluated_event(
        "01HQ456POLLSENSOR",
        Some("cursor_v1"),
        Some("cursor_v2"),
        Some(2),
    );

    state.fold_event(&event);

    let sensor_state = state
        .sensor_state
        .get("01HQ456POLLSENSOR")
        .expect("sensor state should exist");

    assert_eq!(sensor_state.cursor.as_deref(), Some("cursor_v2"));
    assert_eq!(sensor_state.state_version, 3);
}

#[tokio::test]
async fn parity_m2_poll_sensor_cursor_and_min_interval_survive_compactor_reload()
-> arco_flow::error::Result<()> {
    use arco_core::{MemoryBackend, ScopedStorage, WritePrecondition};
    use arco_flow::orchestration::compactor::MicroCompactor;
    use bytes::Bytes;

    fn orchestration_event_path(date: &str, event_id: &str) -> String {
        format!("ledger/orchestration/{date}/{event_id}.json")
    }

    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let sensor_id = "01HQ456POLLSENSOR";
    let eval1_millis = Utc::now().timestamp_millis() - 120_000;
    let eval1_time = Utc
        .timestamp_millis_opt(eval1_millis)
        .single()
        .expect("valid millis timestamp");
    let eval2_time = eval1_time + Duration::seconds(60);

    let compactor = MicroCompactor::new(storage.clone());

    let mut event1 = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: sensor_id.to_string(),
            eval_id: format!("eval_{sensor_id}_01"),
            cursor_before: None,
            cursor_after: Some("cursor_v1".to_string()),
            expected_state_version: None,
            trigger_source: TriggerSource::Poll {
                poll_epoch: eval1_time.timestamp(),
            },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
        eval1_time,
    );
    event1.event_id = "evt_01_sensor_eval".to_string();

    let path1 = orchestration_event_path("2025-01-01", &event1.event_id);
    storage
        .put_raw(
            &path1,
            Bytes::from(serde_json::to_string(&event1).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    compactor.compact_events(vec![path1]).await?;

    let compactor_reloaded = MicroCompactor::new(storage.clone());
    let (_, state1) = compactor_reloaded.load_state().await?;
    let row1 = state1
        .sensor_state
        .get(sensor_id)
        .expect("sensor state should exist");

    assert_eq!(row1.cursor.as_deref(), Some("cursor_v1"));
    assert_eq!(row1.last_evaluation_at, Some(eval1_time));
    assert_eq!(row1.state_version, 1);

    let controller = PollSensorController::with_min_interval(Duration::seconds(30));
    assert!(!controller.should_evaluate(row1, eval1_time + Duration::seconds(10)));
    assert!(controller.should_evaluate(row1, eval1_time + Duration::seconds(31)));

    let mut event2 = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: sensor_id.to_string(),
            eval_id: format!("eval_{sensor_id}_02"),
            cursor_before: Some("cursor_v1".to_string()),
            cursor_after: Some("cursor_v2".to_string()),
            expected_state_version: Some(row1.state_version),
            trigger_source: TriggerSource::Poll {
                poll_epoch: eval2_time.timestamp(),
            },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
        eval2_time,
    );
    event2.event_id = "evt_02_sensor_eval".to_string();

    let path2 = orchestration_event_path("2025-01-01", &event2.event_id);
    storage
        .put_raw(
            &path2,
            Bytes::from(serde_json::to_string(&event2).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    compactor_reloaded.compact_events(vec![path2]).await?;

    let compactor_reloaded_again = MicroCompactor::new(storage.clone());
    let (_, state2) = compactor_reloaded_again.load_state().await?;
    let row2 = state2
        .sensor_state
        .get(sensor_id)
        .expect("sensor state should exist");

    assert_eq!(row2.cursor.as_deref(), Some("cursor_v2"));
    assert_eq!(row2.last_evaluation_at, Some(eval2_time));
    assert_eq!(row2.state_version, 2);

    let eval3_time = eval2_time + Duration::seconds(60);
    let stale_eval_id = format!("eval_{sensor_id}_stale");
    let mut event3 = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: sensor_id.to_string(),
            eval_id: stale_eval_id.clone(),
            cursor_before: Some("cursor_v1".to_string()),
            cursor_after: Some("cursor_v3".to_string()),
            expected_state_version: Some(1),
            trigger_source: TriggerSource::Poll {
                poll_epoch: eval3_time.timestamp(),
            },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
        eval3_time,
    );
    event3.event_id = "evt_03_sensor_eval".to_string();

    let path3 = orchestration_event_path("2025-01-01", &event3.event_id);
    storage
        .put_raw(
            &path3,
            Bytes::from(serde_json::to_string(&event3).expect("serialize")),
            WritePrecondition::None,
        )
        .await?;
    compactor_reloaded_again.compact_events(vec![path3]).await?;

    let compactor_final = MicroCompactor::new(storage);
    let (_, state3) = compactor_final.load_state().await?;
    let row3 = state3
        .sensor_state
        .get(sensor_id)
        .expect("sensor state should exist");

    assert_eq!(row3.cursor.as_deref(), Some("cursor_v2"));
    assert_eq!(row3.last_evaluation_at, Some(eval2_time));
    assert_eq!(row3.state_version, 2);

    let eval3 = state3
        .sensor_evals
        .get(stale_eval_id.as_str())
        .expect("stale eval row should be recorded");
    assert!(matches!(eval3.status, SensorEvalStatus::SkippedStaleCursor));

    Ok(())
}

#[test]
fn parity_m2_backfill_pause_resume_cancel_transitions_are_monotonic() {
    let resolver = Arc::new(StaticPartitionResolver::new(vec!["2025-01-01".into()]));
    let controller = BackfillController::new(resolver);

    let current = BackfillRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        backfill_id: "bf_001".into(),
        asset_selection: vec!["analytics.daily".into()],
        partition_selector: arco_flow::orchestration::events::PartitionSelector::Explicit {
            partition_keys: vec!["2025-01-01".into()],
        },
        chunk_size: 2,
        max_concurrent_runs: 1,
        state: BackfillState::Running,
        state_version: 7,
        total_partitions: 1,
        planned_chunks: 0,
        completed_chunks: 0,
        failed_chunks: 0,
        parent_backfill_id: None,
        created_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        row_version: "row_01".into(),
    };

    let paused = controller
        .pause("bf_001", &current, "tenant-abc", "workspace-prod")
        .expect("pause should succeed");

    assert_eq!(paused.idempotency_key, "backfill_state:bf_001:8:PAUSED");
    let OrchestrationEventData::BackfillStateChanged {
        from_state,
        to_state,
        state_version,
        ..
    } = &paused.data
    else {
        panic!("expected BackfillStateChanged");
    };
    assert_eq!(*from_state, BackfillState::Running);
    assert_eq!(*to_state, BackfillState::Paused);
    assert_eq!(*state_version, 8);

    let paused_row = BackfillRow {
        state: BackfillState::Paused,
        state_version: 8,
        ..current.clone()
    };

    let resumed = controller
        .resume("bf_001", &paused_row, "tenant-abc", "workspace-prod")
        .expect("resume should succeed");

    assert_eq!(resumed.idempotency_key, "backfill_state:bf_001:9:RUNNING");
    let OrchestrationEventData::BackfillStateChanged {
        from_state,
        to_state,
        state_version,
        ..
    } = &resumed.data
    else {
        panic!("expected BackfillStateChanged");
    };
    assert_eq!(*from_state, BackfillState::Paused);
    assert_eq!(*to_state, BackfillState::Running);
    assert_eq!(*state_version, 9);

    let cancelled = controller
        .cancel("bf_001", &current, "tenant-abc", "workspace-prod")
        .expect("cancel should succeed");

    assert_eq!(
        cancelled.idempotency_key,
        "backfill_state:bf_001:8:CANCELLED"
    );
}

#[test]
fn parity_m2_retry_failed_only_targets_failed_partitions_deterministically() {
    let resolver = Arc::new(StaticPartitionResolver::new(vec!["2025-01-01".into()]));
    let controller = BackfillController::new(resolver);

    let parent = BackfillRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        backfill_id: "bf_parent".into(),
        asset_selection: vec!["analytics.daily".into()],
        partition_selector: arco_flow::orchestration::events::PartitionSelector::Range {
            start: "2025-01-01".into(),
            end: "2025-01-03".into(),
        },
        chunk_size: 2,
        max_concurrent_runs: 1,
        state: BackfillState::Failed,
        state_version: 3,
        total_partitions: 3,
        planned_chunks: 2,
        completed_chunks: 1,
        failed_chunks: 1,
        parent_backfill_id: None,
        created_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        row_version: "row_parent".into(),
    };

    let failed_chunks = vec![BackfillChunkRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        chunk_id: "bf_parent:0".into(),
        backfill_id: "bf_parent".into(),
        chunk_index: 0,
        partition_keys: vec![
            "2025-01-02".into(),
            "2025-01-01".into(),
            "2025-01-01".into(),
        ],
        run_key: "backfill:bf_parent:chunk:0".into(),
        run_id: None,
        state: ChunkState::Failed,
        row_version: "row_chunk".into(),
    }];

    let event = controller
        .retry_failed(
            "bf_retry",
            &parent,
            &failed_chunks,
            "retry_req_001",
            "tenant-abc",
            "workspace-prod",
        )
        .expect("retry_failed should succeed");

    assert_eq!(
        event.idempotency_key,
        "backfill_retry:bf_parent:retry_req_001"
    );

    let OrchestrationEventData::BackfillCreated {
        backfill_id,
        parent_backfill_id,
        partition_selector,
        ..
    } = &event.data
    else {
        panic!("expected BackfillCreated");
    };

    assert_eq!(backfill_id, "bf_retry");
    assert_eq!(parent_backfill_id.as_deref(), Some("bf_parent"));

    let arco_flow::orchestration::events::PartitionSelector::Explicit { partition_keys } =
        partition_selector
    else {
        panic!("expected explicit partition selector");
    };

    assert_eq!(
        partition_keys,
        &vec!["2025-01-01".to_string(), "2025-01-02".to_string()]
    );
}

#[test]
fn parity_m2_schedule_sensor_backfill_and_manual_reexecution_share_run_key_consistency() {
    let mut state = FoldState::new();
    let run_key = "rk_shared_q3";
    let fingerprint = "fp_shared_q3";

    let sources = vec![
        SourceRef::Schedule {
            schedule_id: "daily".to_string(),
            tick_id: "tick_1".to_string(),
        },
        SourceRef::Sensor {
            sensor_id: "sensor_1".to_string(),
            eval_id: "eval_1".to_string(),
        },
        SourceRef::Backfill {
            backfill_id: "bf_1".to_string(),
            chunk_id: "chunk_1".to_string(),
        },
        SourceRef::Manual {
            user_id: "operator@example.com".to_string(),
            request_id: "rerun_req_1".to_string(),
        },
    ];

    for source in sources {
        state.fold_event(&OrchestrationEvent::new(
            "tenant-abc",
            "workspace-prod",
            OrchestrationEventData::RunRequested {
                run_key: run_key.to_string(),
                request_fingerprint: fingerprint.to_string(),
                asset_selection: vec!["analytics.daily".to_string()],
                partition_selection: Some(vec!["date=2025-01-15".to_string()]),
                trigger_source_ref: source,
                labels: HashMap::new(),
            },
        ));
    }

    assert_eq!(state.run_key_index.len(), 1);
    assert!(
        state.run_key_conflicts.is_empty(),
        "equivalent run requests from schedule/sensor/backfill/manual should dedupe cleanly"
    );

    state.fold_event(&OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: run_key.to_string(),
            request_fingerprint: "fp_conflict_q3".to_string(),
            asset_selection: vec!["analytics.other".to_string()],
            partition_selection: Some(vec!["date=2025-01-16".to_string()]),
            trigger_source_ref: SourceRef::Manual {
                user_id: "operator@example.com".to_string(),
                request_id: "rerun_req_2".to_string(),
            },
            labels: HashMap::new(),
        },
    ));

    assert_eq!(state.run_key_conflicts.len(), 1);
    let conflict = state
        .run_key_conflicts
        .values()
        .next()
        .expect("expected one conflict row");
    assert_eq!(conflict.run_key, run_key);
    assert_eq!(conflict.existing_fingerprint, fingerprint);
    assert_eq!(conflict.conflicting_fingerprint, "fp_conflict_q3");
}
