//! Schedule controller for cron-based automation.
//!
//! This controller evaluates schedule definitions against current state and emits
//! `ScheduleTicked` events for due ticks. Per P0-1, it also emits `RunRequested`
//! events atomically with each triggered tick.
//!
//! ## Event Flow
//!
//! ```text
//! Cloud Tasks Timer → ScheduleController.reconcile()
//!                          ↓
//!                    ScheduleTicked + RunRequested (atomic batch)
//!                          ↓
//!                    Compactor → schedule_state, schedule_ticks, runs
//! ```
//!
//! ## Design Principles
//!
//! 1. **Stateless**: Can be scaled horizontally
//! 2. **Idempotent**: Same tick produces same `tick_id`
//! 3. **Atomic emission**: Tick and `RunRequested` in same batch (P0-1)
//! 4. **Snapshot-based**: Embeds `asset_selection` in tick for replay determinism

use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use cron::Schedule;
use metrics::{counter, histogram};
use tracing::warn;

use crate::metrics::{TimingGuard, labels as metrics_labels, names as metrics_names};
use crate::orchestration::compactor::fold::{ScheduleDefinitionRow, ScheduleStateRow};
use crate::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, SourceRef, TickStatus, sha256_hex,
};

/// Schedule controller for reconciling schedule definitions to tick events.
///
/// Evaluates cron expressions and emits `ScheduleTicked` + `RunRequested` events
/// for due ticks. Implements catch-up behavior for missed ticks within the
/// configured window.
pub struct ScheduleController {
    /// Default catch-up window if not specified in definition.
    default_catchup_window: Duration,
}

impl Default for ScheduleController {
    fn default() -> Self {
        Self::new()
    }
}

impl ScheduleController {
    /// Creates a new schedule controller with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            default_catchup_window: Duration::hours(24),
        }
    }

    /// Reconciles schedule definitions against current state, returning events to emit.
    ///
    /// For each enabled schedule with due ticks:
    /// 1. Computes due ticks based on cron expression and last tick time
    /// 2. Emits `ScheduleTicked` event for each due tick
    /// 3. Emits `RunRequested` event atomically with each triggered tick (P0-1)
    ///
    /// # Arguments
    ///
    /// * `definitions` - Schedule definitions to evaluate
    /// * `state` - Current schedule state (last tick times)
    /// * `now` - Current time for evaluation
    ///
    /// # Returns
    ///
    /// Vector of events to append to the ledger atomically.
    #[must_use]
    pub fn reconcile(
        &self,
        definitions: &[ScheduleDefinitionRow],
        state: &[ScheduleStateRow],
        now: DateTime<Utc>,
    ) -> Vec<OrchestrationEvent> {
        let _guard = TimingGuard::new(|duration| {
            histogram!(
                metrics_names::ORCH_CONTROLLER_RECONCILE_SECONDS,
                metrics_labels::CONTROLLER => "schedule".to_string(),
            )
            .record(duration.as_secs_f64());
        });

        let state_map: HashMap<_, _> = state.iter().map(|s| (s.schedule_id.as_str(), s)).collect();

        let mut events = Vec::new();
        let mut actions_total = 0_u64;

        for def in definitions {
            let (mut def_events, def_actions) = self.reconcile_definition(def, &state_map, now);
            actions_total = actions_total.saturating_add(def_actions);
            events.append(&mut def_events);
        }

        counter!(
            metrics_names::ORCH_CONTROLLER_ACTIONS_TOTAL,
            metrics_labels::CONTROLLER => "schedule".to_string(),
        )
        .increment(actions_total);

        events
    }

    fn reconcile_definition(
        &self,
        def: &ScheduleDefinitionRow,
        state_map: &HashMap<&str, &ScheduleStateRow>,
        now: DateTime<Utc>,
    ) -> (Vec<OrchestrationEvent>, u64) {
        let mut events = Vec::new();
        let mut actions_total = 0_u64;

        let schedule = match Self::parse_cron_expression(&def.cron_expression) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    schedule_id = %def.schedule_id,
                    cron_expression = %def.cron_expression,
                    "invalid cron expression: {e}"
                );
                let error_tick = Self::error_tick(def, &e, now);
                Self::push_tick_event(&mut events, &mut actions_total, error_tick);
                return (events, actions_total);
            }
        };

        let Ok(tz): Result<Tz, _> = def.timezone.parse() else {
            warn!(
                schedule_id = %def.schedule_id,
                timezone = %def.timezone,
                "invalid timezone"
            );
            let error_tick =
                Self::error_tick(def, &format!("invalid timezone: {}", def.timezone), now);
            Self::push_tick_event(&mut events, &mut actions_total, error_tick);
            return (events, actions_total);
        };

        let last_scheduled_for = state_map
            .get(def.schedule_id.as_str())
            .and_then(|s| s.last_scheduled_for);

        let catchup_window = if def.catchup_window_minutes == 0 {
            self.default_catchup_window
        } else {
            Duration::minutes(i64::from(def.catchup_window_minutes))
        };

        let ticks = Self::compute_due_ticks(
            &schedule,
            last_scheduled_for,
            now,
            def.max_catchup_ticks,
            catchup_window,
            tz,
        );

        for scheduled_for in ticks {
            let emitted_at = Utc::now();
            let tick_event = if def.enabled {
                Self::create_tick_event(def, scheduled_for, emitted_at)
            } else {
                Self::create_paused_tick_event(def, scheduled_for, emitted_at)
            };
            let run_req = Self::build_run_request(def, &tick_event);
            Self::push_tick_event(&mut events, &mut actions_total, tick_event);
            if let Some(run_req) = run_req {
                Self::push_run_request_event(&mut events, &mut actions_total, run_req);
            }
        }

        (events, actions_total)
    }

    /// Parses a cron expression, normalizing 5-field syntax to 6-field with seconds.
    fn parse_cron_expression(expression: &str) -> Result<Schedule, String> {
        let field_count = expression.split_whitespace().count();
        let normalized = match field_count {
            5 => format!("0 {expression}"),
            6 => expression.to_string(),
            _ => {
                return Err(format!(
                    "invalid cron expression (expected 5 or 6 fields): {expression}"
                ));
            }
        };
        Schedule::from_str(&normalized).map_err(|e| format!("invalid cron expression: {e}"))
    }

    /// Computes due ticks for a schedule.
    fn compute_due_ticks(
        schedule: &Schedule,
        last_scheduled_for: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
        max_catchup: u32,
        catchup_window: Duration,
        tz: Tz,
    ) -> Vec<DateTime<Utc>> {
        let mut start =
            last_scheduled_for.map_or(now - catchup_window, |t| t + Duration::seconds(1));
        let min_start = now - catchup_window;
        if start < min_start {
            start = min_start;
        }

        let start_tz = start.with_timezone(&tz);
        let now_tz = now.with_timezone(&tz);

        schedule
            .after(&start_tz)
            .take_while(|t| *t <= now_tz)
            .take(max_catchup as usize)
            .map(|t| t.with_timezone(&Utc))
            .collect()
    }

    /// Creates a triggered tick event.
    fn create_tick_event(
        def: &ScheduleDefinitionRow,
        scheduled_for: DateTime<Utc>,
        emitted_at: DateTime<Utc>,
    ) -> OrchestrationEvent {
        let tick_id = format!("{}:{}", def.schedule_id, scheduled_for.timestamp());
        let run_key = format!("sched:{}:{}", def.schedule_id, scheduled_for.timestamp());
        let fingerprint = compute_request_fingerprint(&def.asset_selection, None);

        OrchestrationEvent::new_with_timestamp(
            &def.tenant_id,
            &def.workspace_id,
            OrchestrationEventData::ScheduleTicked {
                schedule_id: def.schedule_id.clone(),
                scheduled_for,
                tick_id,
                definition_version: def.row_version.clone(),
                asset_selection: def.asset_selection.clone(),
                partition_selection: None,
                status: TickStatus::Triggered,
                run_key: Some(run_key),
                request_fingerprint: Some(fingerprint),
            },
            emitted_at,
        )
    }

    /// Creates a paused tick event for visible history.
    fn create_paused_tick_event(
        def: &ScheduleDefinitionRow,
        scheduled_for: DateTime<Utc>,
        emitted_at: DateTime<Utc>,
    ) -> OrchestrationEvent {
        let tick_id = format!("{}:{}", def.schedule_id, scheduled_for.timestamp());

        OrchestrationEvent::new_with_timestamp(
            &def.tenant_id,
            &def.workspace_id,
            OrchestrationEventData::ScheduleTicked {
                schedule_id: def.schedule_id.clone(),
                scheduled_for,
                tick_id,
                definition_version: def.row_version.clone(),
                asset_selection: def.asset_selection.clone(),
                partition_selection: None,
                status: TickStatus::Skipped {
                    reason: "paused".to_string(),
                },
                run_key: None,
                request_fingerprint: None,
            },
            emitted_at,
        )
    }

    fn push_tick_event(
        events: &mut Vec<OrchestrationEvent>,
        actions_total: &mut u64,
        event: OrchestrationEvent,
    ) {
        Self::record_tick_metrics(&event);
        events.push(event);
        *actions_total = actions_total.saturating_add(1);
    }

    fn push_run_request_event(
        events: &mut Vec<OrchestrationEvent>,
        actions_total: &mut u64,
        event: OrchestrationEvent,
    ) {
        Self::record_run_request_metrics();
        events.push(event);
        *actions_total = actions_total.saturating_add(1);
    }

    fn build_run_request(
        def: &ScheduleDefinitionRow,
        tick_event: &OrchestrationEvent,
    ) -> Option<OrchestrationEvent> {
        let OrchestrationEventData::ScheduleTicked {
            tick_id,
            run_key,
            request_fingerprint,
            asset_selection,
            partition_selection,
            status: TickStatus::Triggered,
            ..
        } = &tick_event.data
        else {
            return None;
        };

        let run_key = run_key.as_ref()?;

        Some(OrchestrationEvent::new_with_timestamp(
            &def.tenant_id,
            &def.workspace_id,
            OrchestrationEventData::RunRequested {
                run_key: run_key.clone(),
                request_fingerprint: request_fingerprint.clone().unwrap_or_default(),
                asset_selection: asset_selection.clone(),
                partition_selection: partition_selection.clone(),
                trigger_source_ref: SourceRef::Schedule {
                    schedule_id: def.schedule_id.clone(),
                    tick_id: tick_id.clone(),
                },
                labels: HashMap::new(),
            },
            tick_event.timestamp,
        ))
    }

    fn record_tick_metrics(event: &OrchestrationEvent) {
        let OrchestrationEventData::ScheduleTicked { status, .. } = &event.data else {
            return;
        };
        counter!(
            metrics_names::SCHEDULE_TICKS_TOTAL,
            metrics_labels::STATUS => Self::tick_status_label(status).to_string(),
        )
        .increment(1);
    }

    fn record_run_request_metrics() {
        counter!(
            metrics_names::RUN_REQUESTS_TOTAL,
            metrics_labels::SOURCE => "schedule".to_string(),
        )
        .increment(1);
    }

    fn tick_status_label(status: &TickStatus) -> &'static str {
        match status {
            TickStatus::Triggered => "triggered",
            TickStatus::Skipped { .. } => "skipped",
            TickStatus::Failed { .. } => "failed",
        }
    }

    /// Creates an error tick event for invalid schedule configuration.
    fn error_tick(
        def: &ScheduleDefinitionRow,
        error: &str,
        now: DateTime<Utc>,
    ) -> OrchestrationEvent {
        let tick_id = format!("{}:{}", def.schedule_id, now.timestamp());

        OrchestrationEvent::new_with_timestamp(
            &def.tenant_id,
            &def.workspace_id,
            OrchestrationEventData::ScheduleTicked {
                schedule_id: def.schedule_id.clone(),
                scheduled_for: now,
                tick_id,
                definition_version: def.row_version.clone(),
                asset_selection: def.asset_selection.clone(),
                partition_selection: None,
                status: TickStatus::Failed {
                    error: error.to_string(),
                },
                run_key: None,
                request_fingerprint: None,
            },
            now,
        )
    }

    /// Manually trigger a schedule (for debugging/testing).
    ///
    /// Creates a triggered tick with a special `manual:` prefix in the `tick_id`.
    #[must_use]
    pub fn manual_trigger(
        def: &ScheduleDefinitionRow,
        now: DateTime<Utc>,
    ) -> Vec<OrchestrationEvent> {
        let tick_id = format!("{}:manual:{}", def.schedule_id, now.timestamp());
        let run_key = format!("sched:{}:manual:{}", def.schedule_id, now.timestamp());
        let fingerprint = compute_request_fingerprint(&def.asset_selection, None);
        let emitted_at = Utc::now();

        let tick_event = OrchestrationEvent::new_with_timestamp(
            &def.tenant_id,
            &def.workspace_id,
            OrchestrationEventData::ScheduleTicked {
                schedule_id: def.schedule_id.clone(),
                scheduled_for: now,
                tick_id: tick_id.clone(),
                definition_version: def.row_version.clone(),
                asset_selection: def.asset_selection.clone(),
                partition_selection: None,
                status: TickStatus::Triggered,
                run_key: Some(run_key.clone()),
                request_fingerprint: Some(fingerprint.clone()),
            },
            emitted_at,
        );

        let run_req = OrchestrationEvent::new_with_timestamp(
            &def.tenant_id,
            &def.workspace_id,
            OrchestrationEventData::RunRequested {
                run_key,
                request_fingerprint: fingerprint,
                asset_selection: def.asset_selection.clone(),
                partition_selection: None,
                trigger_source_ref: SourceRef::Schedule {
                    schedule_id: def.schedule_id.clone(),
                    tick_id,
                },
                labels: HashMap::new(),
            },
            emitted_at,
        );

        vec![tick_event, run_req]
    }
}

/// Computes a request fingerprint from asset and partition selections.
///
/// Used for conflict detection when the same `run_key` is requested with different parameters.
fn compute_request_fingerprint(
    asset_selection: &[String],
    partition_selection: Option<&[String]>,
) -> String {
    let mut assets = asset_selection.to_vec();
    assets.sort();

    let mut input = assets.join(",");
    if let Some(partitions) = partition_selection {
        let mut partitions = partitions.to_vec();
        partitions.sort();

        input.push_str(";partitions:");
        input.push_str(&partitions.join(","));
    }
    sha256_hex(&input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn test_definition(schedule_id: &str) -> ScheduleDefinitionRow {
        ScheduleDefinitionRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            schedule_id: schedule_id.into(),
            // Controller accepts 5- or 6-field cron (seconds optional).
            cron_expression: "0 0 10 * * *".into(), // Daily at 10:00:00
            timezone: "UTC".into(),
            catchup_window_minutes: 60 * 24, // 24 hours
            asset_selection: vec!["analytics.summary".into()],
            max_catchup_ticks: 3,
            enabled: true,
            created_at: Utc::now(),
            row_version: "def_01HQ123".into(),
        }
    }

    #[test]
    fn test_schedule_controller_emits_tick_when_due() {
        let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap();
        let scheduled_for = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();

        let mut def = test_definition("01HQ123SCHEDXYZ");
        def.cron_expression = "0 0 10 * * *".into(); // sec min hour day month dow

        let state = vec![ScheduleStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            schedule_id: "01HQ123SCHEDXYZ".into(),
            last_scheduled_for: Some(scheduled_for - Duration::hours(24)),
            last_tick_id: None,
            last_run_key: None,
            row_version: "state_01HQ".into(),
        }];

        let controller = ScheduleController::new();
        let events = controller.reconcile(&[def], &state, now);

        // Should emit both ScheduleTicked and RunRequested
        assert!(
            events.len() >= 2,
            "Expected at least 2 events (tick + run request), got {}",
            events.len()
        );

        let has_tick = events
            .iter()
            .any(|e| matches!(&e.data, OrchestrationEventData::ScheduleTicked { schedule_id, .. } if schedule_id == "01HQ123SCHEDXYZ"));
        assert!(has_tick, "Should have ScheduleTicked event");

        let has_run_req = events
            .iter()
            .any(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }));
        assert!(has_run_req, "Should have RunRequested event");
    }

    #[test]
    fn test_schedule_controller_respects_max_catchup() {
        let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap();
        // Missed 10 daily ticks
        let last_tick = now - Duration::days(10);

        let mut def = test_definition("01HQ123SCHEDXYZ");
        def.max_catchup_ticks = 3;
        def.catchup_window_minutes = 60 * 24 * 14; // Allow full catchup window

        let state = vec![ScheduleStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            schedule_id: "01HQ123SCHEDXYZ".into(),
            last_scheduled_for: Some(last_tick),
            last_tick_id: None,
            last_run_key: None,
            row_version: "state_01HQ".into(),
        }];

        let controller = ScheduleController::new();
        let events = controller.reconcile(&[def], &state, now);

        // Should emit 3 ticks + 3 run requests = 6 events max
        let tick_count = events
            .iter()
            .filter(|e| matches!(&e.data, OrchestrationEventData::ScheduleTicked { .. }))
            .count();
        assert_eq!(
            tick_count, 3,
            "Should only emit max_catchup_ticks (3) ticks"
        );
    }

    #[test]
    fn test_schedule_controller_emits_paused_tick_when_disabled() {
        let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap();
        let mut def = test_definition("01HQ123SCHEDXYZ");
        def.enabled = false; // Paused

        let controller = ScheduleController::new();
        let events = controller.reconcile(&[def], &[], now);

        let has_paused_tick = events.iter().any(|e| {
            matches!(
                &e.data,
                OrchestrationEventData::ScheduleTicked {
                    status: TickStatus::Skipped { reason },
                    ..
                } if reason == "paused"
            )
        });
        assert!(has_paused_tick, "Should emit paused ScheduleTicked event");

        let has_run_req = events
            .iter()
            .any(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }));
        assert!(!has_run_req, "Paused schedule should not emit RunRequested");
    }

    #[test]
    fn test_schedule_controller_emits_tick_and_run_requested_atomically() {
        let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap();

        let mut def = test_definition("01HQ123SCHEDXYZ");
        def.asset_selection = vec!["asset_b".into()];

        let controller = ScheduleController::new();
        let events = controller.reconcile(&[def], &[], now);

        // Controller must emit BOTH events
        let has_tick = events
            .iter()
            .any(|e| matches!(&e.data, OrchestrationEventData::ScheduleTicked { .. }));
        let has_run_req = events
            .iter()
            .any(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }));

        assert!(has_tick, "Controller must emit ScheduleTicked");
        assert!(has_run_req, "Controller must emit RunRequested with tick");

        // Verify RunRequested uses snapshotted asset_selection
        let run_req = events
            .iter()
            .find(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }));

        if let Some(e) = run_req {
            if let OrchestrationEventData::RunRequested {
                asset_selection, ..
            } = &e.data
            {
                assert_eq!(asset_selection, &vec!["asset_b".to_string()]);
            }
        }
    }

    #[test]
    fn test_schedule_manual_trigger_creates_tick_and_run_request() {
        let def = test_definition("01HQ123SCHEDXYZ");
        let now = Utc::now();

        let events = ScheduleController::manual_trigger(&def, now);

        assert_eq!(
            events.len(),
            2,
            "Manual trigger should emit tick + run request"
        );

        let has_tick = events.iter().any(|e| {
            matches!(
                &e.data,
                OrchestrationEventData::ScheduleTicked {
                    status: TickStatus::Triggered,
                    tick_id,
                    ..
                } if tick_id.contains("manual:")
            )
        });
        assert!(has_tick, "Should have manual tick event");

        let has_run_req = events
            .iter()
            .any(|e| matches!(&e.data, OrchestrationEventData::RunRequested { .. }));
        assert!(has_run_req, "Should have RunRequested event");
    }

    #[test]
    fn test_schedule_ticked_skipped_has_no_run_key() {
        // This tests the error case where tick is failed/skipped
        let mut def = test_definition("01HQ123SCHEDXYZ");
        def.cron_expression = "invalid-cron".into(); // Invalid expression

        let controller = ScheduleController::new();
        let events = controller.reconcile(&[def], &[], Utc::now());

        assert_eq!(events.len(), 1, "Should emit one error tick");

        let event = &events[0];
        if let OrchestrationEventData::ScheduleTicked {
            status, run_key, ..
        } = &event.data
        {
            assert!(
                matches!(status, TickStatus::Failed { .. }),
                "Should be failed status"
            );
            assert!(run_key.is_none(), "Failed tick should have no run_key");
        } else {
            panic!("Expected ScheduleTicked event");
        }
    }

    #[test]
    fn test_schedule_controller_clamps_catchup_window() {
        let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 35, 0).unwrap();
        let last_tick = now - Duration::hours(48);

        let mut def = test_definition("01HQ123SCHEDXYZ");
        def.cron_expression = "0 */10 * * * *".into(); // Every 10 minutes
        def.catchup_window_minutes = 60; // 1 hour
        def.max_catchup_ticks = 1000;

        let state = vec![ScheduleStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            schedule_id: "01HQ123SCHEDXYZ".into(),
            last_scheduled_for: Some(last_tick),
            last_tick_id: None,
            last_run_key: None,
            row_version: "state_01HQ".into(),
        }];

        let controller = ScheduleController::new();
        let events = controller.reconcile(&[def], &state, now);

        let scheduled_fors: Vec<_> = events
            .iter()
            .filter_map(|e| match &e.data {
                OrchestrationEventData::ScheduleTicked { scheduled_for, .. } => {
                    Some(scheduled_for.clone())
                }
                _ => None,
            })
            .collect();

        assert!(
            !scheduled_fors.is_empty(),
            "Expected due ticks within catchup window"
        );

        let min_tick = scheduled_fors
            .iter()
            .min()
            .expect("expected at least one tick");
        assert!(
            *min_tick >= now - Duration::minutes(60),
            "Catchup should clamp to window start"
        );
    }

    #[test]
    fn test_schedule_controller_accepts_five_field_cron() {
        let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 30, 0).unwrap();
        let scheduled_for = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();

        let mut def = test_definition("01HQ123SCHEDXYZ");
        def.cron_expression = "0 10 * * *".into(); // 5-field cron

        let state = vec![ScheduleStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            schedule_id: "01HQ123SCHEDXYZ".into(),
            last_scheduled_for: Some(scheduled_for - Duration::hours(24)),
            last_tick_id: None,
            last_run_key: None,
            row_version: "state_01HQ".into(),
        }];

        let controller = ScheduleController::new();
        let events = controller.reconcile(&[def], &state, now);

        let has_tick = events.iter().any(|e| {
            matches!(
                &e.data,
                OrchestrationEventData::ScheduleTicked {
                    status: TickStatus::Triggered,
                    ..
                }
            )
        });
        assert!(has_tick, "Should emit tick for 5-field cron");
    }

    #[test]
    fn test_schedule_controller_handles_dst_fall_back() {
        let now = Utc.with_ymd_and_hms(2025, 11, 2, 7, 30, 0).unwrap();
        let last_tick = Utc.with_ymd_and_hms(2025, 11, 2, 4, 0, 0).unwrap(); // 00:00 EDT

        let mut def = test_definition("01HQ123SCHEDXYZ");
        def.cron_expression = "0 0 * * * *".into(); // Top of every hour
        def.timezone = "America/New_York".into();
        def.catchup_window_minutes = 360;
        def.max_catchup_ticks = 10;

        let state = vec![ScheduleStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            schedule_id: "01HQ123SCHEDXYZ".into(),
            last_scheduled_for: Some(last_tick),
            last_tick_id: None,
            last_run_key: None,
            row_version: "state_01HQ".into(),
        }];

        let controller = ScheduleController::new();
        let events = controller.reconcile(&[def], &state, now);

        let scheduled_fors: Vec<_> = events
            .iter()
            .filter_map(|e| match &e.data {
                OrchestrationEventData::ScheduleTicked { scheduled_for, .. } => {
                    Some(scheduled_for.clone())
                }
                _ => None,
            })
            .collect();

        let expected = vec![
            Utc.with_ymd_and_hms(2025, 11, 2, 5, 0, 0).unwrap(), // 01:00 EDT
            Utc.with_ymd_and_hms(2025, 11, 2, 6, 0, 0).unwrap(), // 01:00 EST
            Utc.with_ymd_and_hms(2025, 11, 2, 7, 0, 0).unwrap(), // 02:00 EST
        ];
        assert_eq!(
            scheduled_fors, expected,
            "DST fall-back should yield repeated hour ticks"
        );
    }

    #[test]
    fn test_tick_id_format_uses_stable_id() {
        let now = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();
        let epoch = now.timestamp();

        let def = test_definition("01HQ123SCHEDXYZ");

        let events = ScheduleController::manual_trigger(&def, now);

        let tick_event = events
            .iter()
            .find(|e| matches!(&e.data, OrchestrationEventData::ScheduleTicked { .. }));

        if let Some(e) = tick_event {
            if let OrchestrationEventData::ScheduleTicked { tick_id, .. } = &e.data {
                // Tick ID should use schedule_id (ULID), not human-readable name
                assert!(
                    tick_id.starts_with("01HQ123SCHEDXYZ:"),
                    "tick_id should use schedule_id ULID: {}",
                    tick_id
                );
                assert!(
                    tick_id.contains(&format!("manual:{epoch}")),
                    "tick_id should contain epoch: {}",
                    tick_id
                );
            }
        }
    }

    #[test]
    fn test_request_fingerprint_is_deterministic() {
        let assets = vec!["a".to_string(), "b".to_string()];
        let fp1 = compute_request_fingerprint(&assets, None);
        let fp2 = compute_request_fingerprint(&assets, None);
        assert_eq!(fp1, fp2, "Fingerprint should be deterministic");

        let reversed_assets = vec!["b".to_string(), "a".to_string()];
        let fp3 = compute_request_fingerprint(&reversed_assets, None);
        assert_eq!(
            fp1, fp3,
            "Fingerprint should be order-insensitive for asset selection"
        );

        let fp4 = compute_request_fingerprint(&assets, Some(&["p1".to_string()]));
        assert_ne!(
            fp1, fp4,
            "Different partitions should produce different fingerprint"
        );
    }
}
