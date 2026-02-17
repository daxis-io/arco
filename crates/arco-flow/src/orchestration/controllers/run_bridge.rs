//! Run request bridge controller.
//!
//! Bridges Layer-2 `RunRequested` projections into concrete `RunTriggered` +
//! `PlanCreated` events for execution.

use std::collections::HashMap;

use chrono::Duration;
use metrics::{counter, histogram};
use sha2::{Digest, Sha256};

use crate::metrics::{TimingGuard, labels as metrics_labels, names as metrics_names};
use crate::orchestration::compactor::fold::FoldState;
use crate::orchestration::compactor::manifest::OrchestrationManifest;
use crate::orchestration::events::{OrchestrationEventData, TaskDef, TriggerInfo};
use crate::orchestration::{
    AssetGraph, SelectionOptions, build_task_defs_for_selection, canonicalize_asset_key,
};

/// Action returned by run request bridge reconciliation.
#[derive(Debug, Clone)]
pub enum RunBridgeAction {
    /// Emit `RunTriggered` + `PlanCreated` events for a pending run request.
    EmitRunEvents {
        /// The run trigger event payload.
        run_triggered: OrchestrationEventData,
        /// The plan creation event payload.
        plan_created: OrchestrationEventData,
    },
    /// Skip this run key with a deterministic reason.
    Skip {
        /// Run key that was skipped.
        run_key: String,
        /// Skip reason.
        reason: String,
    },
}

#[derive(Debug, Clone)]
struct ResolvedRunRequest {
    trigger: TriggerInfo,
    asset_selection: Vec<String>,
    partition_selection: Option<Vec<String>>,
}

type RunKeyLookup = HashMap<String, HashMap<String, RunRequestLookupEntry>>;

#[derive(Debug, Clone)]
struct RunRequestLookupEntry {
    row_version: String,
    request: ResolvedRunRequest,
}

#[derive(Debug, Default, Clone)]
struct RunRequestLookup {
    schedule: RunKeyLookup,
    sensor: RunKeyLookup,
    backfill: RunKeyLookup,
}

impl RunRequestLookup {
    fn build(state: &FoldState) -> Self {
        let mut lookup = Self::default();

        for tick in state.schedule_ticks.values() {
            let (Some(run_key), Some(request_fingerprint)) =
                (tick.run_key.as_deref(), tick.request_fingerprint.as_deref())
            else {
                continue;
            };

            let request = ResolvedRunRequest {
                trigger: TriggerInfo::Cron {
                    schedule_id: tick.schedule_id.clone(),
                },
                asset_selection: tick.asset_selection.clone(),
                partition_selection: tick.partition_selection.clone(),
            };

            Self::upsert_latest(
                &mut lookup.schedule,
                run_key,
                request_fingerprint,
                &tick.row_version,
                request,
            );
        }

        for eval in state.sensor_evals.values() {
            let cursor = eval
                .cursor_after
                .clone()
                .or_else(|| eval.cursor_before.clone())
                .unwrap_or_else(|| "unknown".to_string());
            for req in &eval.run_requests {
                let request = ResolvedRunRequest {
                    trigger: TriggerInfo::Sensor {
                        sensor_id: eval.sensor_id.clone(),
                        cursor: cursor.clone(),
                    },
                    asset_selection: req.asset_selection.clone(),
                    partition_selection: req.partition_selection.clone(),
                };

                Self::upsert_latest(
                    &mut lookup.sensor,
                    &req.run_key,
                    &req.request_fingerprint,
                    &eval.row_version,
                    request,
                );
            }
        }

        for chunk in state.backfill_chunks.values() {
            let request_fingerprint = compute_backfill_request_fingerprint(&chunk.partition_keys);
            let assets = state
                .backfills
                .get(&chunk.backfill_id)
                .map_or_else(Vec::new, |row| row.asset_selection.clone());

            let request = ResolvedRunRequest {
                trigger: TriggerInfo::Manual {
                    user_id: format!("backfill:{}", chunk.backfill_id),
                },
                asset_selection: assets,
                partition_selection: Some(chunk.partition_keys.clone()),
            };

            Self::upsert_latest(
                &mut lookup.backfill,
                &chunk.run_key,
                &request_fingerprint,
                &chunk.row_version,
                request,
            );
        }

        lookup
    }

    fn upsert_latest(
        source: &mut RunKeyLookup,
        run_key: &str,
        request_fingerprint: &str,
        row_version: &str,
        request: ResolvedRunRequest,
    ) {
        let by_fingerprint = source.entry(run_key.to_string()).or_default();
        if by_fingerprint
            .get(request_fingerprint)
            .is_some_and(|existing| existing.row_version.as_str() >= row_version)
        {
            return;
        }
        by_fingerprint.insert(
            request_fingerprint.to_string(),
            RunRequestLookupEntry {
                row_version: row_version.to_string(),
                request,
            },
        );
    }

    fn get(
        source: &RunKeyLookup,
        run_key: &str,
        request_fingerprint: &str,
    ) -> Option<ResolvedRunRequest> {
        source
            .get(run_key)
            .and_then(|by_fingerprint| by_fingerprint.get(request_fingerprint))
            .map(|entry| entry.request.clone())
    }
}

/// Bridges projected `RunRequested` state into executable run events.
///
/// This controller is intentionally projection-only:
/// - Reads from compactor state (`FoldState`)
/// - Emits intent/fact events to ledger
/// - Relies on event idempotency keys for duplicate-safe retries
pub struct RunBridgeController {
    /// Maximum compaction lag before skipping.
    max_compaction_lag: Duration,
}

impl RunBridgeController {
    /// Creates a new run bridge controller.
    #[must_use]
    pub fn new(max_compaction_lag: Duration) -> Self {
        Self { max_compaction_lag }
    }

    /// Creates a run bridge with default settings.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(Duration::seconds(30))
    }

    /// Reconciles pending run requests into run lifecycle events.
    #[must_use]
    pub fn reconcile(
        &self,
        manifest: &OrchestrationManifest,
        state: &FoldState,
    ) -> Vec<RunBridgeAction> {
        let _guard = TimingGuard::new(|duration| {
            histogram!(
                metrics_names::ORCH_CONTROLLER_RECONCILE_SECONDS,
                metrics_labels::CONTROLLER => "run_bridge".to_string(),
            )
            .record(duration.as_secs_f64());
        });

        let mut run_keys: Vec<&str> = state.run_key_index.keys().map(String::as_str).collect();
        run_keys.sort_unstable();
        let lookup = RunRequestLookup::build(state);

        let actions: Vec<RunBridgeAction> = if manifest.watermarks.is_fresh(self.max_compaction_lag)
        {
            run_keys
                .into_iter()
                .filter_map(|run_key| Self::reconcile_run_key(run_key, state, &lookup))
                .collect()
        } else {
            run_keys
                .into_iter()
                .map(|run_key| RunBridgeAction::Skip {
                    run_key: run_key.to_string(),
                    reason: "compaction_lag".to_string(),
                })
                .collect()
        };

        let count = u64::try_from(actions.len()).unwrap_or(0);
        counter!(
            metrics_names::ORCH_CONTROLLER_ACTIONS_TOTAL,
            metrics_labels::CONTROLLER => "run_bridge".to_string(),
        )
        .increment(count);

        actions
    }

    fn reconcile_run_key(
        run_key: &str,
        state: &FoldState,
        lookup: &RunRequestLookup,
    ) -> Option<RunBridgeAction> {
        let index = state.run_key_index.get(run_key)?;

        if state.runs.contains_key(&index.run_id) {
            return Some(RunBridgeAction::Skip {
                run_key: run_key.to_string(),
                reason: "run_already_exists".to_string(),
            });
        }

        let Some(request) = resolve_run_request(run_key, &index.request_fingerprint, lookup) else {
            return Some(RunBridgeAction::Skip {
                run_key: run_key.to_string(),
                reason: "run_request_source_not_found".to_string(),
            });
        };

        let plan_id = format!("plan:{}", index.run_id);
        let tasks = build_tasks(
            &request.asset_selection,
            request.partition_selection.as_deref(),
        );

        let run_triggered = OrchestrationEventData::RunTriggered {
            run_id: index.run_id.clone(),
            plan_id: plan_id.clone(),
            trigger: request.trigger,
            root_assets: request.asset_selection,
            run_key: Some(run_key.to_string()),
            labels: HashMap::new(),
            code_version: None,
        };

        let plan_created = OrchestrationEventData::PlanCreated {
            run_id: index.run_id.clone(),
            plan_id,
            tasks,
        };

        Some(RunBridgeAction::EmitRunEvents {
            run_triggered,
            plan_created,
        })
    }
}

fn resolve_run_request(
    run_key: &str,
    request_fingerprint: &str,
    lookup: &RunRequestLookup,
) -> Option<ResolvedRunRequest> {
    RunRequestLookup::get(&lookup.schedule, run_key, request_fingerprint)
        .or_else(|| RunRequestLookup::get(&lookup.sensor, run_key, request_fingerprint))
        .or_else(|| RunRequestLookup::get(&lookup.backfill, run_key, request_fingerprint))
}

fn compute_backfill_request_fingerprint(partition_keys: &[String]) -> String {
    let mut hasher = Sha256::new();
    for key in partition_keys {
        hasher.update(key.as_bytes());
        hasher.update(b"|");
    }
    let result = hasher.finalize();
    let bytes: [u8; 16] = result
        .get(..16)
        .and_then(|slice| slice.try_into().ok())
        .unwrap_or([0_u8; 16]);
    hex::encode(bytes)
}

fn build_tasks(asset_selection: &[String], partition_selection: Option<&[String]>) -> Vec<TaskDef> {
    let mut graph = AssetGraph::new();
    let mut roots = Vec::new();
    for asset in asset_selection {
        if let Ok(canonical) = canonicalize_asset_key(asset) {
            graph.insert_asset(canonical.clone(), Vec::new());
            roots.push(canonical);
        }
    }

    let partition_key = partition_selection.and_then(|keys| match keys {
        [single] => Some(single.as_str()),
        _ => None,
    });

    if !roots.is_empty() {
        let mut sorted_roots = roots.clone();
        sorted_roots.sort();
        sorted_roots.dedup();
        if let Ok(tasks) = build_task_defs_for_selection(
            &graph,
            &sorted_roots,
            SelectionOptions::none(),
            partition_key,
        ) {
            if !tasks.is_empty() {
                return tasks;
            }
        }
    }

    vec![TaskDef {
        key: "materialize".to_string(),
        depends_on: Vec::new(),
        asset_key: None,
        partition_key: None,
        max_attempts: 3,
        heartbeat_timeout_sec: 300,
    }]
}
