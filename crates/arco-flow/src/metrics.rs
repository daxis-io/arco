//! Observability metrics for orchestration.
//!
//! This module provides Prometheus-compatible metrics for monitoring
//! the orchestration engine. Metrics are designed to support:
//!
//! - **Alerting**: SLO-based alerts on task latency and failure rates
//! - **Dashboards**: Real-time visibility into scheduler health
//! - **Debugging**: Correlating metrics with traces for root cause analysis
//!
//! ## Metrics Exported
//!
//! | Metric | Type | Labels | Description |
//! |--------|------|--------|-------------|
//! | `arco_flow_tasks_total` | Counter | `from_state`, `to_state` | Total task state transitions |
//! | `arco_flow_task_duration_seconds` | Histogram | operation, state | Task execution duration |
//! | `arco_flow_scheduler_tick_duration_seconds` | Histogram | - | Scheduler tick processing time |
//! | `arco_flow_active_runs` | Gauge | - | Currently active runs |
//! | `arco_flow_dispatch_queue_depth` | Gauge | queue | Tasks waiting in dispatch queue |
//! | `arco_flow_schedule_ticks_total` | Counter | status | Schedule tick outcomes |
//! | `arco_flow_run_requests_total` | Counter | source | Run requests by trigger source |
//!
//! ## Usage
//!
//! ```rust,no_run
//! use arco_flow::metrics::FlowMetrics;
//!
//! let metrics = FlowMetrics::new();
//!
//! // Record task state transition
//! metrics.record_task_transition("ready", "running");
//!
//! // Record task execution time
//! metrics.observe_task_duration("materialize", "succeeded", 45.2);
//!
//! // Update active runs gauge
//! metrics.set_active_runs(5);
//! ```
//!
//! ## Integration
//!
//! Metrics are exposed via the `metrics` crate facade. To export to Prometheus:
//!
//! ```rust,ignore
//! use metrics_exporter_prometheus::PrometheusBuilder;
//!
//! PrometheusBuilder::new()
//!     .with_http_listener(([0, 0, 0, 0], 9090))
//!     .install()
//!     .expect("failed to install Prometheus recorder");
//! ```

use std::time::{Duration, Instant};

use metrics::{counter, gauge, histogram};

/// Metric names as constants for consistency.
pub mod names {
    /// Counter: Total task state transitions.
    pub const TASKS_TOTAL: &str = "arco_flow_tasks_total";
    /// Histogram: Task execution duration in seconds.
    pub const TASK_DURATION_SECONDS: &str = "arco_flow_task_duration_seconds";
    /// Histogram: Scheduler tick processing time in seconds.
    pub const SCHEDULER_TICK_DURATION_SECONDS: &str = "arco_flow_scheduler_tick_duration_seconds";
    /// Gauge: Total currently active runs.
    pub const ACTIVE_RUNS: &str = "arco_flow_active_runs";
    /// Gauge: Tasks waiting in dispatch queue.
    pub const DISPATCH_QUEUE_DEPTH: &str = "arco_flow_dispatch_queue_depth";
    /// Counter: Total dispatch operations.
    pub const DISPATCHES_TOTAL: &str = "arco_flow_dispatches_total";
    /// Counter: Total retry operations.
    pub const RETRIES_TOTAL: &str = "arco_flow_retries_total";
    /// Counter: Total orchestration callback requests.
    pub const ORCH_CALLBACKS_TOTAL: &str = "arco_orch_callbacks_total";
    /// Counter: Total orchestration callback errors.
    pub const ORCH_CALLBACK_ERRORS_TOTAL: &str = "arco_orch_callback_errors_total";
    /// Histogram: Orchestration callback latency in seconds.
    pub const ORCH_CALLBACK_DURATION_SECONDS: &str = "arco_orch_callback_duration_seconds";
    /// Counter: Orchestration controller actions emitted.
    pub const ORCH_CONTROLLER_ACTIONS_TOTAL: &str = "arco_orch_controller_actions_total";
    /// Histogram: Orchestration controller reconcile duration in seconds.
    pub const ORCH_CONTROLLER_RECONCILE_SECONDS: &str = "arco_orch_controller_reconcile_seconds";
    /// Counter: Schedule ticks by outcome.
    pub const SCHEDULE_TICKS_TOTAL: &str = "arco_flow_schedule_ticks_total";
    /// Counter: Run requests by source.
    pub const RUN_REQUESTS_TOTAL: &str = "arco_flow_run_requests_total";
    /// Counter: Sensor evaluations by type and status.
    pub const SENSOR_EVALS_TOTAL: &str = "arco_flow_sensor_evals_total";
    /// Counter: Orchestration compaction acknowledgements.
    pub const ORCH_COMPACTIONS_TOTAL: &str = "arco_flow_orch_compactions_total";
    /// Histogram: End-to-end compaction acknowledgement latency in seconds.
    pub const ORCH_COMPACTOR_ACK_LATENCY_SECONDS: &str =
        "arco_flow_orch_compactor_ack_latency_seconds";
    /// Gauge: Visibility lag measured as committed-vs-visible event skew.
    pub const ORCH_COMPACTOR_VISIBILITY_LAG_EVENTS: &str =
        "arco_flow_orch_compactor_visibility_lag_events";
}

/// Label keys used across metrics.
pub mod labels {
    /// Task state (ready, running, succeeded, failed, etc.).
    pub const STATE: &str = "state";
    /// Previous task state (for transitions).
    pub const FROM_STATE: &str = "from_state";
    /// Target task state (for transitions).
    pub const TO_STATE: &str = "to_state";
    /// Operation type (materialize, check, backfill).
    pub const OPERATION: &str = "operation";
    /// Queue name for dispatch metrics.
    pub const QUEUE: &str = "queue";
    /// Result status (success, failure, deduplicated).
    pub const RESULT: &str = "result";
    /// Callback handler name.
    pub const HANDLER: &str = "handler";
    /// Controller name.
    pub const CONTROLLER: &str = "controller";
    /// Outcome status (triggered, skipped, failed).
    pub const STATUS: &str = "status";
    /// Trigger source (schedule, sensor, backfill, manual).
    pub const SOURCE: &str = "source";
    /// Sensor type (push, poll).
    pub const SENSOR_TYPE: &str = "sensor_type";
    /// Durability mode label.
    pub const DURABILITY_MODE: &str = "durability_mode";
}

/// High-level interface for recording orchestration metrics.
///
/// This struct provides ergonomic methods for recording metrics
/// with proper labeling. It's designed to be cheap to clone
/// and share across tasks.
#[derive(Debug, Clone, Default)]
pub struct FlowMetrics {
    /// Optional prefix for metric names (for multi-tenant deployments).
    _prefix: Option<String>,
}

impl FlowMetrics {
    /// Creates a new metrics recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a task state transition.
    ///
    /// Increments the `arco_flow_tasks_total` counter with transition labels.
    pub fn record_task_transition(&self, from_state: &str, to_state: &str) {
        counter!(
            names::TASKS_TOTAL,
            labels::FROM_STATE => from_state.to_string(),
            labels::TO_STATE => to_state.to_string(),
        )
        .increment(1);
    }

    /// Records task execution duration.
    ///
    /// Records the duration in the `arco_flow_task_duration_seconds` histogram.
    pub fn observe_task_duration(&self, operation: &str, final_state: &str, duration_secs: f64) {
        histogram!(
            names::TASK_DURATION_SECONDS,
            labels::OPERATION => operation.to_string(),
            labels::STATE => final_state.to_string(),
        )
        .record(duration_secs);
    }

    /// Records scheduler tick duration.
    ///
    /// Records the duration in the `arco_flow_scheduler_tick_duration_seconds` histogram.
    pub fn observe_scheduler_tick_duration(&self, duration: Duration) {
        histogram!(names::SCHEDULER_TICK_DURATION_SECONDS).record(duration.as_secs_f64());
    }

    /// Sets the total number of active runs.
    ///
    /// Updates the `arco_flow_active_runs` gauge.
    #[allow(clippy::cast_precision_loss)] // Gauge values are typically small
    pub fn set_active_runs(&self, count: usize) {
        gauge!(names::ACTIVE_RUNS).set(count as f64);
    }

    /// Sets the dispatch queue depth.
    ///
    /// Updates the `arco_flow_dispatch_queue_depth` gauge.
    #[allow(clippy::cast_precision_loss)] // Gauge values are typically small
    pub fn set_queue_depth(&self, queue: &str, depth: usize) {
        gauge!(
            names::DISPATCH_QUEUE_DEPTH,
            labels::QUEUE => queue.to_string(),
        )
        .set(depth as f64);
    }

    /// Records a dispatch operation.
    ///
    /// Increments the `arco_flow_dispatches_total` counter.
    pub fn record_dispatch(&self, result: &str) {
        counter!(
            names::DISPATCHES_TOTAL,
            labels::RESULT => result.to_string(),
        )
        .increment(1);
    }

    /// Records a retry operation.
    ///
    /// Increments the `arco_flow_retries_total` counter.
    pub fn record_retry(&self, attempt: u32) {
        counter!(
            names::RETRIES_TOTAL,
            "attempt" => attempt.to_string(),
        )
        .increment(1);
    }
}

/// RAII guard for timing operations.
///
/// Automatically records duration when dropped.
///
/// ## Example
///
/// ```rust,no_run
/// use arco_flow::metrics::{FlowMetrics, TimingGuard};
///
/// let metrics = FlowMetrics::new();
///
/// {
///     let _guard = TimingGuard::new(|duration| {
///         metrics.observe_scheduler_tick_duration(duration);
///     });
///
///     // Do work...
/// } // Duration recorded automatically on drop
/// ```
pub struct TimingGuard<F>
where
    F: FnOnce(Duration),
{
    start: Instant,
    on_drop: Option<F>,
}

impl<F> TimingGuard<F>
where
    F: FnOnce(Duration),
{
    /// Creates a new timing guard that will call `on_drop` with the elapsed duration.
    pub fn new(on_drop: F) -> Self {
        Self {
            start: Instant::now(),
            on_drop: Some(on_drop),
        }
    }

    /// Returns the elapsed time since the guard was created.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl<F> Drop for TimingGuard<F>
where
    F: FnOnce(Duration),
{
    fn drop(&mut self) {
        if let Some(f) = self.on_drop.take() {
            f(self.start.elapsed());
        }
    }
}

/// Creates a timing guard for scheduler tick metrics.
///
/// ## Example
///
/// ```rust,no_run
/// use arco_flow::metrics::time_scheduler_tick;
///
/// async fn scheduler_tick() {
///     let _guard = time_scheduler_tick();
///     // Process tick...
/// }
/// ```
#[must_use]
pub fn time_scheduler_tick() -> TimingGuard<impl FnOnce(Duration)> {
    TimingGuard::new(|duration| {
        histogram!(names::SCHEDULER_TICK_DURATION_SECONDS).record(duration.as_secs_f64());
    })
}

/// Creates a timing guard for task execution metrics.
///
/// The `on_complete` callback receives the duration and should record the metric
/// with appropriate labels (operation, final state).
#[must_use]
pub fn time_task_execution<F>(on_complete: F) -> TimingGuard<F>
where
    F: FnOnce(Duration),
{
    TimingGuard::new(on_complete)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flow_metrics_can_record_transitions() {
        let metrics = FlowMetrics::new();

        // These calls should not panic even without a metrics recorder installed
        metrics.record_task_transition("ready", "running");
        metrics.record_task_transition("running", "succeeded");
    }

    #[test]
    fn flow_metrics_can_observe_durations() {
        let metrics = FlowMetrics::new();

        metrics.observe_task_duration("materialize", "succeeded", 1.5);
        metrics.observe_scheduler_tick_duration(Duration::from_millis(100));
    }

    #[test]
    fn flow_metrics_can_set_gauges() {
        let metrics = FlowMetrics::new();

        metrics.set_active_runs(5);
        metrics.set_queue_depth("default", 10);
    }

    #[test]
    fn timing_guard_measures_duration() {
        let mut recorded_duration = None;

        {
            let _guard = TimingGuard::new(|d| {
                recorded_duration = Some(d);
            });
            std::thread::sleep(Duration::from_millis(10));
        }

        // Duration should have been recorded
        assert!(recorded_duration.is_some());
        assert!(recorded_duration.is_some_and(|d| d >= Duration::from_millis(10)));
    }

    #[test]
    fn timing_guard_elapsed_works() {
        let guard = TimingGuard::new(|_| {});
        std::thread::sleep(Duration::from_millis(5));
        let elapsed = guard.elapsed();
        assert!(elapsed >= Duration::from_millis(5));
    }
}
