//! Parity gate suite M3: staleness/reconciliation + advanced UX invariants.
//!
//! Focus: deterministic staleness computation and core “at-least-once” safety properties.

use std::collections::HashMap;

use chrono::{TimeZone, Utc};

use arco_flow::orchestration::compactor::FoldState;
use arco_flow::orchestration::compactor::fold::PartitionStatusRow;
use arco_flow::orchestration::controllers::partition_status::{
    FreshnessPolicy, StalenessReason, compute_staleness, compute_staleness_with_upstreams,
};
use arco_flow::orchestration::events::{OrchestrationEvent, OrchestrationEventData, SourceRef};

fn make_partition(
    asset_key: &str,
    partition_key: &str,
    last_mat_at: Option<chrono::DateTime<Utc>>,
    code_version: Option<&str>,
) -> PartitionStatusRow {
    PartitionStatusRow {
        tenant_id: "tenant".into(),
        workspace_id: "workspace".into(),
        asset_key: asset_key.into(),
        partition_key: partition_key.into(),
        last_materialization_run_id: last_mat_at.as_ref().map(|_| "run_123".into()),
        last_materialization_at: last_mat_at,
        last_materialization_code_version: code_version.map(ToString::to_string),
        last_attempt_run_id: None,
        last_attempt_at: None,
        last_attempt_outcome: None,
        stale_since: None,
        stale_reason_code: None,
        partition_values: HashMap::new(),
        row_version: "v1".into(),
    }
}

fn run_requested_event(run_key: &str, fingerprint: &str) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant",
        "workspace",
        OrchestrationEventData::RunRequested {
            run_key: run_key.to_string(),
            request_fingerprint: fingerprint.to_string(),
            asset_selection: vec!["analytics.daily".to_string()],
            partition_selection: None,
            trigger_source_ref: SourceRef::Schedule {
                schedule_id: "sched_01".to_string(),
                tick_id: "tick_01".to_string(),
            },
            labels: HashMap::new(),
        },
    )
}

#[test]
fn parity_m3_staleness_never_materialized_is_stale_with_reason_code() {
    let now = Utc.with_ymd_and_hms(2025, 1, 1, 12, 0, 0).unwrap();
    let partition = make_partition("analytics.daily", "2025-01-01", None, None);

    let result = compute_staleness(&partition, &FreshnessPolicy::default(), Some("v1"), now);

    assert!(result.is_stale);
    assert_eq!(result.reason, Some(StalenessReason::NeverMaterialized));
    assert!(result.stale_since.is_none());
}

#[test]
fn parity_m3_staleness_code_version_change_is_stale() {
    let now = Utc.with_ymd_and_hms(2025, 1, 1, 12, 0, 0).unwrap();
    let last_mat = Utc.with_ymd_and_hms(2025, 1, 1, 11, 0, 0).unwrap();
    let partition = make_partition(
        "analytics.daily",
        "2025-01-01",
        Some(last_mat),
        Some("code_v1"),
    );

    let result = compute_staleness(
        &partition,
        &FreshnessPolicy::default(),
        Some("code_v2"),
        now,
    );

    assert!(result.is_stale);
    assert_eq!(result.reason, Some(StalenessReason::CodeChanged));
    assert_eq!(result.stale_since, Some(now));
}

#[test]
fn parity_m3_staleness_upstream_newer_marks_downstream_stale() {
    let now = Utc.with_ymd_and_hms(2025, 1, 1, 12, 0, 0).unwrap();

    let downstream_last_mat = Utc.with_ymd_and_hms(2025, 1, 1, 9, 0, 0).unwrap();
    let upstream_last_mat = Utc.with_ymd_and_hms(2025, 1, 1, 10, 0, 0).unwrap();

    let downstream = make_partition(
        "analytics.summary",
        "2025-01",
        Some(downstream_last_mat),
        Some("v1"),
    );
    let upstream = make_partition(
        "analytics.daily",
        "2025-01-01",
        Some(upstream_last_mat),
        Some("v1"),
    );

    let result = compute_staleness_with_upstreams(
        &downstream,
        &[upstream],
        &FreshnessPolicy::default(),
        Some("v1"),
        now,
    );

    assert!(result.is_stale);
    assert_eq!(result.reason, Some(StalenessReason::UpstreamChanged));
    assert_eq!(result.stale_since, Some(upstream_last_mat));
}

#[test]
fn parity_m3_global_idempotency_gate_drops_duplicate_idempotency_keys() {
    let mut state = FoldState::new();

    let event = run_requested_event("sched:daily-etl:1736935200", "fingerprint_v1");

    state.fold_event(&event);
    state.fold_event(&event);

    assert_eq!(state.idempotency_keys.len(), 1);
    assert_eq!(state.run_key_index.len(), 1);
    assert!(state.run_key_conflicts.is_empty());
}
