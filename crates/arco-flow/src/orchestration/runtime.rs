//! Runtime configuration and observability snapshots for orchestration services.
//!
//! These settings and snapshot helpers make runtime limits explicit and
//! reproducible for operators and readiness audits.

use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration, Utc};

use crate::error::{Error, Result};
use crate::orchestration::compactor::fold::{DispatchStatus, FoldState, TimerState};
use crate::orchestration::compactor::manifest::OrchestrationManifest;

const ENV_MAX_COMPACTION_LAG_SECS: &str = "ARCO_FLOW_ORCH_MAX_COMPACTION_LAG_SECS";
const ENV_SLO_P95_RUN_REQUESTED_TO_TRIGGERED_SECS: &str =
    "ARCO_FLOW_ORCH_SLO_P95_RUN_REQUESTED_TO_TRIGGERED_SECS";
const ENV_SLO_P95_COMPACTION_LAG_SECS: &str = "ARCO_FLOW_ORCH_SLO_P95_COMPACTION_LAG_SECS";

const DEFAULT_MAX_COMPACTION_LAG_SECS: u64 = 30;
const DEFAULT_SLO_P95_RUN_REQUESTED_TO_TRIGGERED_SECS: u64 = 10;
const DEFAULT_SLO_P95_COMPACTION_LAG_SECS: u64 = 30;

/// Runtime limit and SLO configuration for orchestration controllers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrchestrationRuntimeConfig {
    /// Maximum acceptable compaction lag before controllers skip reconcile work.
    pub max_compaction_lag: Duration,
    /// Target P95 latency from `RunRequested` to `RunTriggered`.
    pub slo_p95_run_requested_to_triggered: StdDuration,
    /// Target P95 compaction watermark lag for controller reads.
    pub slo_p95_compaction_lag: Duration,
}

impl Default for OrchestrationRuntimeConfig {
    fn default() -> Self {
        Self {
            max_compaction_lag: Duration::seconds(
                i64::try_from(DEFAULT_MAX_COMPACTION_LAG_SECS).unwrap_or(30),
            ),
            slo_p95_run_requested_to_triggered: StdDuration::from_secs(
                DEFAULT_SLO_P95_RUN_REQUESTED_TO_TRIGGERED_SECS,
            ),
            slo_p95_compaction_lag: Duration::seconds(
                i64::try_from(DEFAULT_SLO_P95_COMPACTION_LAG_SECS).unwrap_or(30),
            ),
        }
    }
}

impl OrchestrationRuntimeConfig {
    /// Loads runtime config from process environment with strict validation.
    ///
    /// The values must be positive integer seconds when provided.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when an environment value is not a
    /// positive integer or exceeds the supported range.
    pub fn from_env() -> Result<Self> {
        Self::from_env_with(|key| std::env::var(key).ok())
    }

    /// Loads runtime config with a custom environment source.
    ///
    /// This entry point is test-friendly and accepts a key lookup function.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when an environment value is not a
    /// positive integer or exceeds the supported range.
    pub fn from_env_with<F>(get_env: F) -> Result<Self>
    where
        F: Fn(&str) -> Option<String>,
    {
        let defaults = Self::default();
        let max_compaction_lag_secs = parse_positive_u64_env(
            &get_env,
            ENV_MAX_COMPACTION_LAG_SECS,
            DEFAULT_MAX_COMPACTION_LAG_SECS,
        )?;
        let run_requested_to_triggered_secs = parse_positive_u64_env(
            &get_env,
            ENV_SLO_P95_RUN_REQUESTED_TO_TRIGGERED_SECS,
            DEFAULT_SLO_P95_RUN_REQUESTED_TO_TRIGGERED_SECS,
        )?;
        let compaction_lag_slo_secs = parse_positive_u64_env(
            &get_env,
            ENV_SLO_P95_COMPACTION_LAG_SECS,
            DEFAULT_SLO_P95_COMPACTION_LAG_SECS,
        )?;

        let max_compaction_lag =
            Duration::seconds(i64::try_from(max_compaction_lag_secs).map_err(|_| {
                Error::configuration(format!(
                    "{ENV_MAX_COMPACTION_LAG_SECS} value {max_compaction_lag_secs} exceeds supported range"
                ))
            })?);
        let slo_p95_compaction_lag =
            Duration::seconds(i64::try_from(compaction_lag_slo_secs).map_err(|_| {
                Error::configuration(format!(
                    "{ENV_SLO_P95_COMPACTION_LAG_SECS} value {compaction_lag_slo_secs} exceeds supported range"
                ))
            })?);

        Ok(Self {
            max_compaction_lag,
            slo_p95_run_requested_to_triggered: StdDuration::from_secs(
                run_requested_to_triggered_secs,
            ),
            slo_p95_compaction_lag,
        }
        .with_default_fallback(defaults))
    }

    fn with_default_fallback(self, defaults: Self) -> Self {
        // Defensive guard against accidental zero durations from conversions.
        if self.max_compaction_lag <= Duration::zero()
            || self.slo_p95_run_requested_to_triggered.is_zero()
            || self.slo_p95_compaction_lag <= Duration::zero()
        {
            defaults
        } else {
            self
        }
    }
}

fn parse_positive_u64_env<F>(get_env: &F, key: &str, default: u64) -> Result<u64>
where
    F: Fn(&str) -> Option<String>,
{
    let Some(raw) = get_env(key) else {
        return Ok(default);
    };

    let parsed = raw.parse::<u64>().map_err(|_| {
        Error::configuration(format!("{key} must be a positive integer, got '{raw}'"))
    })?;
    if parsed == 0 {
        return Err(Error::configuration(format!(
            "{key} must be greater than zero"
        )));
    }
    Ok(parsed)
}

/// Derived backlog/lag/conflict snapshot for operator metrics.
#[derive(Debug, Clone, PartialEq)]
pub struct OrchestrationBacklogSnapshot {
    /// Run-request bridge backlog count (run keys without materialized runs).
    pub run_bridge_pending: usize,
    /// Dispatch outbox backlog count.
    pub dispatch_outbox_pending: usize,
    /// Pending timer rows count.
    pub timer_pending: usize,
    /// Total durable run-key conflict rows.
    pub run_key_conflicts: usize,
    /// Compaction watermark lag in seconds.
    pub compaction_lag_seconds: f64,
}

/// Runtime SLO snapshot derived from current orchestration state.
#[derive(Debug, Clone, PartialEq)]
pub struct OrchestrationSloSnapshot {
    /// Observed compaction lag in seconds.
    pub compaction_lag_seconds: f64,
    /// Configured compaction lag p95 target in seconds.
    pub compaction_lag_target_seconds: f64,
    /// Whether compaction lag currently breaches its target.
    pub compaction_lag_breached: bool,
    /// Observed p95 `RunRequested -> RunTriggered` latency in seconds.
    pub run_requested_to_triggered_p95_seconds: Option<f64>,
    /// Configured `RunRequested -> RunTriggered` p95 target in seconds.
    pub run_requested_to_triggered_target_seconds: f64,
    /// Whether run-request latency currently breaches its target.
    pub run_requested_to_triggered_breached: bool,
}

/// Computes a current backlog snapshot from manifest + fold state.
#[must_use]
pub fn snapshot_backlog(
    now: DateTime<Utc>,
    manifest: &OrchestrationManifest,
    state: &FoldState,
) -> OrchestrationBacklogSnapshot {
    let run_bridge_pending = state
        .run_key_index
        .values()
        .filter(|row| !state.runs.contains_key(&row.run_id))
        .count();

    let lag_ms = (now - manifest.watermarks.last_processed_at)
        .num_milliseconds()
        .max(0);
    let lag_ms_u64 = u64::try_from(lag_ms).unwrap_or(0);
    let compaction_lag_seconds = StdDuration::from_millis(lag_ms_u64).as_secs_f64();

    OrchestrationBacklogSnapshot {
        run_bridge_pending,
        dispatch_outbox_pending: state
            .dispatch_outbox
            .values()
            .filter(|row| row.status == DispatchStatus::Pending)
            .count(),
        timer_pending: state
            .timers
            .values()
            .filter(|row| row.state == TimerState::Scheduled && row.cloud_task_id.is_none())
            .count(),
        run_key_conflicts: state.run_key_conflicts.len(),
        compaction_lag_seconds,
    }
}

/// Computes current SLO compliance from manifest + fold state.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn snapshot_slos(
    now: DateTime<Utc>,
    manifest: &OrchestrationManifest,
    state: &FoldState,
    runtime: &OrchestrationRuntimeConfig,
) -> OrchestrationSloSnapshot {
    let backlog = snapshot_backlog(now, manifest, state);
    let compaction_lag_target_seconds = chrono_duration_seconds(runtime.slo_p95_compaction_lag);
    let run_requested_to_triggered_target_seconds =
        runtime.slo_p95_run_requested_to_triggered.as_secs_f64();

    let mut latency_samples_seconds = Vec::new();
    for index in state.run_key_index.values() {
        let Some(run) = state.runs.get(&index.run_id) else {
            continue;
        };
        let latency_ms = (run.triggered_at - index.created_at)
            .num_milliseconds()
            .max(0);
        latency_samples_seconds.push(latency_ms as f64 / 1_000.0);
    }

    let run_requested_to_triggered_p95_seconds = percentile_p95(latency_samples_seconds);
    let run_requested_to_triggered_breached = run_requested_to_triggered_p95_seconds
        .is_some_and(|observed| observed > run_requested_to_triggered_target_seconds);

    OrchestrationSloSnapshot {
        compaction_lag_seconds: backlog.compaction_lag_seconds,
        compaction_lag_target_seconds,
        compaction_lag_breached: backlog.compaction_lag_seconds > compaction_lag_target_seconds,
        run_requested_to_triggered_p95_seconds,
        run_requested_to_triggered_target_seconds,
        run_requested_to_triggered_breached,
    }
}

#[allow(clippy::cast_precision_loss)]
fn chrono_duration_seconds(duration: Duration) -> f64 {
    let millis = duration.num_milliseconds().max(0);
    millis as f64 / 1_000.0
}

#[must_use]
fn percentile_p95(mut samples: Vec<f64>) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    samples.sort_by(f64::total_cmp);
    let index = (95 * samples.len()).div_ceil(100).saturating_sub(1);
    samples.get(index).copied()
}
