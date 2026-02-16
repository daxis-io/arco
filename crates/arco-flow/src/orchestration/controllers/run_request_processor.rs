//! Runtime processor for bridging `RunRequested` intents into executable runs.
//!
//! This processor reads folded projection state and emits:
//! - `RunTriggered` when the deterministic `run_id` has not been materialized
//! - `PlanCreated` when the run has no task graph yet
//!
//! The processor is idempotent:
//! - `RunTriggered` idempotency is keyed by `run_key`
//! - `PlanCreated` idempotency is keyed by `run_id`

use std::collections::{BTreeSet, HashMap};

use crate::orchestration::compactor::FoldState;
use crate::orchestration::compactor::fold::{BackfillChunkRow, RunKeyIndexRow};
use crate::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TriggerInfo,
};

/// Runtime processor that materializes durable runs from `RunRequested` state.
#[derive(Debug, Clone)]
pub struct RunRequestProcessor {
    default_max_attempts: u32,
    default_heartbeat_timeout_sec: u32,
}

impl Default for RunRequestProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl RunRequestProcessor {
    /// Creates a processor with runtime defaults for planned tasks.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            default_max_attempts: 3,
            default_heartbeat_timeout_sec: 300,
        }
    }

    /// Reconciles pending run intents into `RunTriggered` + `PlanCreated` events.
    #[must_use]
    pub fn reconcile(&self, state: &FoldState) -> Vec<OrchestrationEvent> {
        let mut keys: Vec<&RunKeyIndexRow> = state.run_key_index.values().collect();
        keys.sort_by(|a, b| a.run_key.cmp(&b.run_key));

        let mut events = Vec::new();

        for key in keys {
            let run_exists = state.runs.contains_key(&key.run_id);
            let has_tasks = run_has_tasks(state, &key.run_id);

            if run_exists && has_tasks {
                continue;
            }

            let Some(resolved) = resolve_request_payload(state, key) else {
                tracing::warn!(
                    run_key = %key.run_key,
                    run_id = %key.run_id,
                    "skipping unresolved run request payload"
                );
                continue;
            };

            let tasks = self.build_tasks(
                &resolved.asset_selection,
                resolved.partition_selection.as_ref(),
            );
            if tasks.is_empty() {
                tracing::warn!(
                    run_key = %key.run_key,
                    run_id = %key.run_id,
                    "skipping run request with empty task graph"
                );
                continue;
            }

            let plan_id = deterministic_plan_id(&key.run_id);

            if !run_exists {
                events.push(OrchestrationEvent::new(
                    &resolved.tenant_id,
                    &resolved.workspace_id,
                    OrchestrationEventData::RunTriggered {
                        run_id: key.run_id.clone(),
                        plan_id: plan_id.clone(),
                        trigger: resolved.trigger,
                        root_assets: resolved.asset_selection.clone(),
                        run_key: Some(key.run_key.clone()),
                        labels: resolved.labels.clone(),
                        code_version: None,
                    },
                ));
            }

            if !has_tasks {
                events.push(OrchestrationEvent::new(
                    &resolved.tenant_id,
                    &resolved.workspace_id,
                    OrchestrationEventData::PlanCreated {
                        run_id: key.run_id.clone(),
                        plan_id,
                        tasks,
                    },
                ));
            }
        }

        events
    }

    fn build_tasks(
        &self,
        asset_selection: &[String],
        partition_selection: Option<&Vec<String>>,
    ) -> Vec<TaskDef> {
        let mut assets = BTreeSet::new();
        for asset in asset_selection {
            if !asset.trim().is_empty() {
                assets.insert(asset.clone());
            }
        }

        let partition_key = partition_selection
            .filter(|parts| parts.len() == 1)
            .and_then(|parts| parts.first().cloned());

        assets
            .into_iter()
            .map(|asset| TaskDef {
                key: asset.clone(),
                depends_on: Vec::new(),
                asset_key: Some(asset),
                partition_key: partition_key.clone(),
                max_attempts: self.default_max_attempts,
                heartbeat_timeout_sec: self.default_heartbeat_timeout_sec,
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
struct ResolvedRunRequest {
    tenant_id: String,
    workspace_id: String,
    asset_selection: Vec<String>,
    partition_selection: Option<Vec<String>>,
    trigger: TriggerInfo,
    labels: HashMap<String, String>,
}

fn run_has_tasks(state: &FoldState, run_id: &str) -> bool {
    state.tasks.keys().any(|(r, _)| r == run_id)
}

fn deterministic_plan_id(run_id: &str) -> String {
    format!("plan_{run_id}")
}

fn resolve_request_payload(state: &FoldState, key: &RunKeyIndexRow) -> Option<ResolvedRunRequest> {
    resolve_from_schedule_ticks(state, key)
        .or_else(|| resolve_from_sensor_evals(state, key))
        .or_else(|| resolve_from_backfill_chunks(state, key))
}

fn resolve_from_schedule_ticks(
    state: &FoldState,
    key: &RunKeyIndexRow,
) -> Option<ResolvedRunRequest> {
    let tick = state
        .schedule_ticks
        .values()
        .filter(|tick| tick.run_key.as_deref() == Some(key.run_key.as_str()))
        .filter(|tick| {
            tick.request_fingerprint.as_deref() == Some(key.request_fingerprint.as_str())
        })
        .max_by(|a, b| {
            a.scheduled_for
                .cmp(&b.scheduled_for)
                .then_with(|| a.row_version.cmp(&b.row_version))
        })?;

    Some(ResolvedRunRequest {
        tenant_id: tick.tenant_id.clone(),
        workspace_id: tick.workspace_id.clone(),
        asset_selection: tick.asset_selection.clone(),
        partition_selection: tick.partition_selection.clone(),
        trigger: TriggerInfo::Cron {
            schedule_id: tick.schedule_id.clone(),
        },
        labels: HashMap::new(),
    })
}

fn resolve_from_sensor_evals(
    state: &FoldState,
    key: &RunKeyIndexRow,
) -> Option<ResolvedRunRequest> {
    let eval = state
        .sensor_evals
        .values()
        .filter(|eval| {
            eval.run_requests.iter().any(|req| {
                req.run_key == key.run_key && req.request_fingerprint == key.request_fingerprint
            })
        })
        .max_by(|a, b| {
            a.evaluated_at
                .cmp(&b.evaluated_at)
                .then_with(|| a.row_version.cmp(&b.row_version))
        })?;

    let request = eval.run_requests.iter().find(|req| {
        req.run_key == key.run_key && req.request_fingerprint == key.request_fingerprint
    })?;

    let cursor = eval
        .cursor_after
        .clone()
        .or_else(|| eval.cursor_before.clone())
        .unwrap_or_default();

    Some(ResolvedRunRequest {
        tenant_id: eval.tenant_id.clone(),
        workspace_id: eval.workspace_id.clone(),
        asset_selection: request.asset_selection.clone(),
        partition_selection: request.partition_selection.clone(),
        trigger: TriggerInfo::Sensor {
            sensor_id: eval.sensor_id.clone(),
            cursor,
        },
        labels: HashMap::new(),
    })
}

fn resolve_from_backfill_chunks(
    state: &FoldState,
    key: &RunKeyIndexRow,
) -> Option<ResolvedRunRequest> {
    let chunk = state
        .backfill_chunks
        .values()
        .filter(|chunk| chunk.run_key == key.run_key)
        .max_by(backfill_chunk_order)?;

    let backfill = state.backfills.get(&chunk.backfill_id)?;

    let mut labels = HashMap::new();
    labels.insert("arco.backfill_id".to_string(), chunk.backfill_id.clone());
    labels.insert("arco.backfill_chunk_id".to_string(), chunk.chunk_id.clone());

    Some(ResolvedRunRequest {
        tenant_id: backfill.tenant_id.clone(),
        workspace_id: backfill.workspace_id.clone(),
        asset_selection: backfill.asset_selection.clone(),
        partition_selection: Some(chunk.partition_keys.clone()),
        trigger: TriggerInfo::Manual {
            user_id: "backfill".to_string(),
        },
        labels,
    })
}

fn backfill_chunk_order(a: &&BackfillChunkRow, b: &&BackfillChunkRow) -> std::cmp::Ordering {
    a.chunk_index
        .cmp(&b.chunk_index)
        .then_with(|| a.row_version.cmp(&b.row_version))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    use crate::orchestration::compactor::fold::{RunKeyIndexRow, ScheduleTickRow};
    use crate::orchestration::events::TickStatus;

    #[test]
    fn reconcile_emits_run_triggered_and_plan_created_for_schedule() {
        let mut state = FoldState::new();
        let run_key = "sched:sched_01:1736935200";
        let run_id = "run_sched_01";
        let fp = "fp_sched_01";

        state.run_key_index.insert(
            run_key.to_string(),
            RunKeyIndexRow {
                tenant_id: "tenant-abc".to_string(),
                workspace_id: "workspace-prod".to_string(),
                run_key: run_key.to_string(),
                run_id: run_id.to_string(),
                request_fingerprint: fp.to_string(),
                created_at: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
                row_version: "evt_runreq".to_string(),
            },
        );

        state.schedule_ticks.insert(
            "sched_01:1736935200".to_string(),
            ScheduleTickRow {
                tenant_id: "tenant-abc".to_string(),
                workspace_id: "workspace-prod".to_string(),
                tick_id: "sched_01:1736935200".to_string(),
                schedule_id: "sched_01".to_string(),
                scheduled_for: Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(),
                definition_version: "def_v1".to_string(),
                asset_selection: vec!["analytics.daily_summary".to_string()],
                partition_selection: None,
                status: TickStatus::Triggered,
                run_key: Some(run_key.to_string()),
                run_id: None,
                request_fingerprint: Some(fp.to_string()),
                row_version: "evt_tick".to_string(),
            },
        );

        let processor = RunRequestProcessor::new();
        let events = processor.reconcile(&state);

        assert_eq!(events.len(), 2);
        assert!(matches!(
            events[0].data,
            OrchestrationEventData::RunTriggered { .. }
        ));
        assert!(matches!(
            events[1].data,
            OrchestrationEventData::PlanCreated { .. }
        ));
    }
}
