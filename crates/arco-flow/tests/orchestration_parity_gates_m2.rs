//! Parity gate suite M2: schedules/sensors/backfills workflows.
//!
//! Focus: controller emission semantics (atomic batches), bounded catchup, CAS-friendly
//! poll sensors, and backfill chunk planning invariants.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, TimeZone, Utc};

use arco_flow::orchestration::compactor::fold::{
    BackfillChunkRow, BackfillRow, RunRow, ScheduleDefinitionRow, ScheduleStateRow, SensorStateRow,
};
use arco_flow::orchestration::compactor::manifest::Watermarks;
use arco_flow::orchestration::controllers::backfill::{BackfillController, PartitionResolver};
use arco_flow::orchestration::controllers::schedule::ScheduleController;
use arco_flow::orchestration::controllers::sensor::PollSensorController;
use arco_flow::orchestration::events::{
    BackfillState, OrchestrationEventData, SensorStatus, SourceRef, TickStatus,
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

    assert!(planned_count >= 1);
    assert!(run_req_count >= 1);

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
