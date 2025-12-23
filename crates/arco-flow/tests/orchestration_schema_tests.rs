//! Tests for Layer 2 Parquet schema row types.

use arco_flow::orchestration::compactor::{
    BackfillChunkRow, BackfillRow, PartitionMaterializationStatus, PartitionStatusRow,
    RunKeyConflictRow, RunKeyIndexRow, ScheduleDefinitionRow, ScheduleStateRow, ScheduleTickRow,
    SensorStateRow,
};
use chrono::{TimeZone, Utc};
use std::collections::HashMap;

// ============================================================================
// Schedule Schema Tests
// ============================================================================

#[test]
fn test_schedule_definition_row_schema() {
    let row = ScheduleDefinitionRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        schedule_id: "daily-etl".into(),
        cron_expression: "0 10 * * *".into(),
        timezone: "UTC".into(),
        catchup_window_minutes: 60 * 24,
        asset_selection: vec!["analytics.summary".into()],
        max_catchup_ticks: 3,
        enabled: true,
        created_at: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
        row_version: "01HQ123".into(),
    };

    // Primary key tuple
    assert_eq!(
        row.primary_key(),
        ("tenant-abc", "workspace-prod", "daily-etl")
    );
}

#[test]
fn test_schedule_state_row_schema() {
    let row = ScheduleStateRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        schedule_id: "daily-etl".into(),
        last_scheduled_for: Some(Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap()),
        last_tick_id: Some("daily-etl:1736935200".into()),
        last_run_key: Some("sched:daily-etl:1736935200".into()),
        row_version: "01HQ456".into(),
    };

    assert_eq!(
        row.primary_key(),
        ("tenant-abc", "workspace-prod", "daily-etl")
    );
}

#[test]
fn test_schedule_tick_row_schema() {
    use arco_flow::orchestration::events::TickStatus;

    let row = ScheduleTickRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        tick_id: "daily-etl:1736935200".into(),
        schedule_id: "daily-etl".into(),
        scheduled_for: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
        definition_version: "01HQ789".into(),
        asset_selection: vec!["analytics.summary".into()],
        partition_selection: None,
        status: TickStatus::Triggered,
        run_key: Some("sched:daily-etl:1736935200".into()),
        run_id: Some("run_01HQ123".into()),
        request_fingerprint: Some("fp123".into()),
        row_version: "01HQ101".into(),
    };

    // Primary key for tick history
    assert_eq!(
        row.primary_key(),
        ("tenant-abc", "workspace-prod", "daily-etl:1736935200")
    );
}

// ============================================================================
// Sensor Schema Tests
// ============================================================================

#[test]
fn test_sensor_state_row_schema() {
    use arco_flow::orchestration::events::SensorStatus;

    let row = SensorStateRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        sensor_id: "file-sensor".into(),
        cursor: Some("gs://bucket/path/file.parquet".into()),
        last_evaluation_at: Some(Utc.with_ymd_and_hms(2025, 1, 15, 10, 5, 0).unwrap()),
        last_eval_id: Some("eval_01HQ123".into()),
        status: SensorStatus::Active,
        state_version: 5,
        row_version: "01HQ202".into(),
    };

    assert_eq!(
        row.primary_key(),
        ("tenant-abc", "workspace-prod", "file-sensor")
    );
}

// ============================================================================
// Backfill Schema Tests
// ============================================================================

#[test]
fn test_backfill_row_schema() {
    use arco_flow::orchestration::events::{BackfillState, PartitionSelector};

    let row = BackfillRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        backfill_id: "bf_01HQ123".into(),
        asset_selection: vec!["analytics.summary".into()],
        partition_selector: PartitionSelector::Range {
            start: "2025-01-01".into(),
            end: "2025-12-31".into(),
        },
        chunk_size: 10,
        max_concurrent_runs: 5,
        state: BackfillState::Running,
        state_version: 2,
        total_partitions: 365,
        planned_chunks: 10,
        completed_chunks: 5,
        failed_chunks: 0,
        parent_backfill_id: None,
        created_at: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
        row_version: "01HQ303".into(),
    };

    assert_eq!(
        row.primary_key(),
        ("tenant-abc", "workspace-prod", "bf_01HQ123")
    );
}

#[test]
fn test_backfill_chunk_row_schema() {
    use arco_flow::orchestration::events::ChunkState;

    let row = BackfillChunkRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        chunk_id: "bf_01HQ123:5".into(),
        backfill_id: "bf_01HQ123".into(),
        chunk_index: 5,
        partition_keys: vec!["2025-01-05".into(), "2025-01-06".into()],
        run_key: "backfill:bf_01HQ123:chunk:5".into(),
        run_id: Some("run_01HQ555".into()),
        state: ChunkState::Running,
        row_version: "01HQ404".into(),
    };

    assert_eq!(
        row.primary_key(),
        ("tenant-abc", "workspace-prod", "bf_01HQ123:5")
    );
}

// ============================================================================
// Partition Status Schema Tests
// ============================================================================

#[test]
fn test_partition_status_row_schema() {
    let mut partition_values = HashMap::new();
    partition_values.insert("date".into(), "2025-01-15".into());

    let row = PartitionStatusRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        asset_key: "analytics.summary".into(),
        partition_key: "2025-01-15".into(),
        status: PartitionMaterializationStatus::Materialized,
        last_materialization_run_id: Some("run_01HQ123".into()),
        last_materialization_at: Some(Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap()),
        last_materialization_code_version: Some("v1.2.3".into()),
        last_attempt_run_id: Some("run_01HQ123".into()),
        last_attempt_at: Some(Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap()),
        last_attempt_outcome: Some("SUCCEEDED".into()),
        stale_since: None,
        stale_reason_code: None,
        partition_values,
        row_version: "01HQ505".into(),
    };

    assert_eq!(
        row.primary_key(),
        ("tenant-abc", "workspace-prod", "analytics.summary", "2025-01-15")
    );
}

#[test]
fn test_partition_materialization_status_variants() {
    // Test all status variants
    assert!(!PartitionMaterializationStatus::NeverMaterialized.is_materialized());
    assert!(PartitionMaterializationStatus::Materialized.is_materialized());
    assert!(PartitionMaterializationStatus::Stale.is_materialized());
    assert!(PartitionMaterializationStatus::MaterializedButLastAttemptFailed.is_materialized());
}

// ============================================================================
// Run Key Index Schema Tests
// ============================================================================

#[test]
fn test_run_key_index_row_schema() {
    let row = RunKeyIndexRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        run_key: "sched:daily-etl:1736935200".into(),
        run_id: "run_01HQ123".into(),
        request_fingerprint: "fp_abc123".into(),
        created_at: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
        row_version: "01HQ606".into(),
    };

    assert_eq!(
        row.primary_key(),
        ("tenant-abc", "workspace-prod", "sched:daily-etl:1736935200")
    );
}

#[test]
fn test_run_key_conflict_row_schema() {
    let row = RunKeyConflictRow {
        tenant_id: "tenant-abc".into(),
        workspace_id: "workspace-prod".into(),
        run_key: "sched:daily-etl:1736935200".into(),
        existing_fingerprint: "fp_v1".into(),
        conflicting_fingerprint: "fp_v2".into(),
        conflicting_event_id: "evt_01HQ789".into(),
        detected_at: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
    };

    // Conflicts are keyed by run_key + conflicting_event_id
    assert_eq!(
        row.primary_key(),
        (
            "tenant-abc",
            "workspace-prod",
            "sched:daily-etl:1736935200",
            "evt_01HQ789"
        )
    );
}
