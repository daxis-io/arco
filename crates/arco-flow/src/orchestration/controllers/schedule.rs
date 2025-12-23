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

use crate::orchestration::compactor::fold::{ScheduleDefinitionRow, ScheduleStateRow};
use crate::orchestration::events::{
    sha256_hex, OrchestrationEvent, OrchestrationEventData, SourceRef, TickStatus,
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
        let state_map: HashMap<_, _> = state.iter().map(|s| (s.schedule_id.as_str(), s)).collect();

        let mut events = Vec::new();

        for def in definitions {
            if !def.enabled {
                continue;
            }

            let schedule = match Schedule::from_str(&def.cron_expression) {
                Ok(s) => s,
                Err(e) => {
                    events.push(Self::error_tick(def, &e.to_string(), now));
                    continue;
                }
            };

            let Ok(tz): Result<Tz, _> = def.timezone.parse() else {
                events.push(Self::error_tick(
                    def,
                    &format!("invalid timezone: {}", def.timezone),
                    now,
                ));
                continue;
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
                // Emit ScheduleTicked
                let tick_event = Self::create_tick_event(def, scheduled_for);

                // Per P0-1: Controller must emit RunRequested atomically with tick
                // Extract values before moving tick_event
                let (should_emit_run_req, tick_id_clone, run_key_clone, fingerprint_clone, assets_clone, partitions_clone) =
                    if let OrchestrationEventData::ScheduleTicked {
                        ref tick_id,
                        ref run_key,
                        ref request_fingerprint,
                        ref asset_selection,
                        ref partition_selection,
                        status: TickStatus::Triggered,
                        ..
                    } = tick_event.data
                    {
                        (
                            run_key.is_some(),
                            tick_id.clone(),
                            run_key.clone(),
                            request_fingerprint.clone(),
                            asset_selection.clone(),
                            partition_selection.clone(),
                        )
                    } else {
                        (false, String::new(), None, None, Vec::new(), None)
                    };

                events.push(tick_event);

                // Emit RunRequested for triggered ticks
                if should_emit_run_req {
                    if let Some(rk) = run_key_clone {
                        let run_req = OrchestrationEvent::new(
                            &def.tenant_id,
                            &def.workspace_id,
                            OrchestrationEventData::RunRequested {
                                run_key: rk,
                                request_fingerprint: fingerprint_clone.unwrap_or_default(),
                                asset_selection: assets_clone,
                                partition_selection: partitions_clone,
                                trigger_source_ref: SourceRef::Schedule {
                                    schedule_id: def.schedule_id.clone(),
                                    tick_id: tick_id_clone,
                                },
                                labels: HashMap::new(),
                            },
                        );
                        events.push(run_req);
                    }
                }
            }
        }

        events
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
        let start = last_scheduled_for
            .map_or(now - catchup_window, |t| t + Duration::seconds(1));

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
    fn create_tick_event(def: &ScheduleDefinitionRow, scheduled_for: DateTime<Utc>) -> OrchestrationEvent {
        let tick_id = format!("{}:{}", def.schedule_id, scheduled_for.timestamp());
        let run_key = format!("sched:{}:{}", def.schedule_id, scheduled_for.timestamp());
        let fingerprint = compute_request_fingerprint(&def.asset_selection, None);

        OrchestrationEvent::new(
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
        )
    }

    /// Creates an error tick event for invalid schedule configuration.
    fn error_tick(def: &ScheduleDefinitionRow, error: &str, now: DateTime<Utc>) -> OrchestrationEvent {
        let tick_id = format!("{}:error:{}", def.schedule_id, now.timestamp());

        OrchestrationEvent::new(
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
        )
    }

    /// Manually trigger a schedule (for debugging/testing).
    ///
    /// Creates a triggered tick with a special `manual:` prefix in the `tick_id`.
    #[must_use]
    pub fn manual_trigger(def: &ScheduleDefinitionRow, now: DateTime<Utc>) -> Vec<OrchestrationEvent> {
        let tick_id = format!("{}:manual:{}", def.schedule_id, now.timestamp());
        let run_key = format!("sched:{}:manual:{}", def.schedule_id, now.timestamp());
        let fingerprint = compute_request_fingerprint(&def.asset_selection, None);

        let tick_event = OrchestrationEvent::new(
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
        );

        let run_req = OrchestrationEvent::new(
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
    let mut input = asset_selection.join(",");
    if let Some(partitions) = partition_selection {
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
            // cron crate uses 6-field format: sec min hour day month day-of-week
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
        assert_eq!(tick_count, 3, "Should only emit max_catchup_ticks (3) ticks");
    }

    #[test]
    fn test_schedule_controller_skips_paused_schedule() {
        let mut def = test_definition("01HQ123SCHEDXYZ");
        def.enabled = false; // Paused

        let controller = ScheduleController::new();
        let events = controller.reconcile(&[def], &[], Utc::now());

        assert!(events.is_empty(), "Should not emit events for paused schedule");
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
            if let OrchestrationEventData::RunRequested { asset_selection, .. } = &e.data {
                assert_eq!(asset_selection, &vec!["asset_b".to_string()]);
            }
        }
    }

    #[test]
    fn test_schedule_manual_trigger_creates_tick_and_run_request() {
        let def = test_definition("01HQ123SCHEDXYZ");
        let now = Utc::now();

        let events = ScheduleController::manual_trigger(&def, now);

        assert_eq!(events.len(), 2, "Manual trigger should emit tick + run request");

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
        if let OrchestrationEventData::ScheduleTicked { status, run_key, .. } = &event.data {
            assert!(matches!(status, TickStatus::Failed { .. }), "Should be failed status");
            assert!(run_key.is_none(), "Failed tick should have no run_key");
        } else {
            panic!("Expected ScheduleTicked event");
        }
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

        let fp3 = compute_request_fingerprint(&assets, Some(&["p1".to_string()]));
        assert_ne!(fp1, fp3, "Different partitions should produce different fingerprint");
    }
}
