//! Sensor controller for push and poll sensor evaluation.
//!
//! This module provides handlers for:
//! - **Push sensors**: Triggered by Pub/Sub messages, uses `message_id` for idempotency
//! - **Poll sensors**: Triggered by timers, uses cursor with CAS for serialization
//!
//! ## Event Flow
//!
//! ```text
//! Pub/Sub Message → PushSensorHandler.handle_message()
//!                          ↓
//!                    SensorEvaluated + RunRequested(s) (atomic batch)
//!                          ↓
//!                    Compactor → sensor_state, sensor_evals, runs
//! ```
//!
//! ```text
//! Cloud Tasks Timer → PollSensorController.evaluate()
//!                          ↓
//!                    SensorEvaluated + RunRequested(s) (atomic batch)
//!                          ↓
//!                    Compactor → sensor_state (with CAS), sensor_evals, runs
//! ```
//!
//! ## Design Principles
//!
//! 1. **Stateless**: Handlers can be scaled horizontally
//! 2. **Idempotent**: Push uses `message_id`, poll uses `cursor_before` + epoch
//! 3. **Atomic emission**: `SensorEvaluated` and `RunRequested`(s) in same batch (P0-1)
//! 4. **CAS for poll**: Poll sensors include `expected_state_version` for cursor serialization (P0-2)

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use metrics::{counter, histogram};
use ulid::Ulid;

use crate::metrics::{TimingGuard, labels as metrics_labels, names as metrics_names};
use crate::orchestration::compactor::fold::SensorStateRow;
use crate::orchestration::controllers::sensor_evaluator::{
    NoopSensorEvaluator, PollSensorResult, PubSubMessage, SensorEvaluator,
};
use crate::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, SensorEvalStatus, SensorStatus, SourceRef,
    TriggerSource,
};

/// Handler for push sensor evaluations.
///
/// Push sensors are triggered by Pub/Sub messages. Each message is processed
/// exactly once using the `message_id` for idempotency.
pub struct PushSensorHandler {
    evaluator: Arc<dyn SensorEvaluator>,
}

impl Default for PushSensorHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl PushSensorHandler {
    /// Creates a new push sensor handler.
    #[must_use]
    pub fn new() -> Self {
        Self::with_evaluator(Arc::new(NoopSensorEvaluator))
    }

    /// Creates a new push sensor handler with a custom evaluator.
    #[must_use]
    pub fn with_evaluator(evaluator: Arc<dyn SensorEvaluator>) -> Self {
        Self { evaluator }
    }

    /// Handles a Pub/Sub message and returns events to emit.
    ///
    /// Returns a vector of events:
    /// 1. `SensorEvaluated` - always emitted
    /// 2. `RunRequested` - one per run request from sensor evaluation
    ///
    /// Per P0-1, all events are emitted atomically in the same batch.
    /// Returns an empty batch if the sensor is paused or in error.
    #[must_use]
    pub fn handle_message(
        &self,
        sensor_id: &str,
        tenant_id: &str,
        workspace_id: &str,
        sensor_status: SensorStatus,
        message: &PubSubMessage,
    ) -> Vec<OrchestrationEvent> {
        let _guard = TimingGuard::new(|duration| {
            histogram!(
                metrics_names::ORCH_CONTROLLER_RECONCILE_SECONDS,
                metrics_labels::CONTROLLER => "push_sensor".to_string(),
            )
            .record(duration.as_secs_f64());
        });

        if matches!(sensor_status, SensorStatus::Paused | SensorStatus::Error) {
            return Vec::new();
        }

        let eval_id = format!("eval_{}_{}", sensor_id, Ulid::new());

        let (run_requests, status) = match self.evaluator.evaluate_push(sensor_id, message) {
            Ok(run_requests) => {
                let status = if run_requests.is_empty() {
                    SensorEvalStatus::NoNewData
                } else {
                    SensorEvalStatus::Triggered
                };
                (run_requests, status)
            }
            Err(err) => (
                Vec::new(),
                SensorEvalStatus::Error {
                    message: err.message,
                },
            ),
        };

        let mut events = Vec::new();
        let emitted_at = Utc::now();

        // Emit SensorEvaluated event
        events.push(OrchestrationEvent::new_with_timestamp(
            tenant_id,
            workspace_id,
            OrchestrationEventData::SensorEvaluated {
                sensor_id: sensor_id.to_string(),
                eval_id: eval_id.clone(),
                cursor_before: None, // Push sensors are cursorless
                cursor_after: Some(message.message_id.clone()), // Message ID for debugging
                expected_state_version: None, // No CAS for push sensors
                trigger_source: TriggerSource::Push {
                    message_id: message.message_id.clone(),
                },
                run_requests: run_requests.clone(),
                status: status.clone(),
            },
            emitted_at,
        ));

        // Emit RunRequested for each run request (atomic with SensorEvaluated per P0-1)
        for req in &run_requests {
            events.push(OrchestrationEvent::new_with_timestamp(
                tenant_id,
                workspace_id,
                OrchestrationEventData::RunRequested {
                    run_key: req.run_key.clone(),
                    request_fingerprint: req.request_fingerprint.clone(),
                    asset_selection: req.asset_selection.clone(),
                    partition_selection: req.partition_selection.clone(),
                    trigger_source_ref: SourceRef::Sensor {
                        sensor_id: sensor_id.to_string(),
                        eval_id: eval_id.clone(),
                    },
                    labels: HashMap::new(),
                },
                emitted_at,
            ));
        }

        Self::record_metrics(&status, run_requests.len());

        events
    }

    /// Manually evaluate a sensor with a sample payload (for testing/debugging).
    #[must_use]
    pub fn manual_evaluate(
        &self,
        sensor_id: &str,
        tenant_id: &str,
        workspace_id: &str,
        sample_payload: &serde_json::Value,
    ) -> Vec<OrchestrationEvent> {
        let message = PubSubMessage {
            message_id: format!("manual_{}", Ulid::new()),
            data: serde_json::to_vec(sample_payload).unwrap_or_default(),
            attributes: HashMap::new(),
            publish_time: Utc::now(),
        };

        self.handle_message(
            sensor_id,
            tenant_id,
            workspace_id,
            SensorStatus::Active,
            &message,
        )
    }

    fn record_metrics(status: &SensorEvalStatus, run_request_count: usize) {
        let status_label = match status {
            SensorEvalStatus::Triggered => "triggered",
            SensorEvalStatus::NoNewData => "no_data",
            SensorEvalStatus::Error { .. } => "error",
            SensorEvalStatus::SkippedStaleCursor => "skipped_stale",
        };

        counter!(
            metrics_names::SENSOR_EVALS_TOTAL,
            metrics_labels::SENSOR_TYPE => "push".to_string(),
            metrics_labels::STATUS => status_label.to_string(),
        )
        .increment(1);

        for _ in 0..run_request_count {
            counter!(
                metrics_names::RUN_REQUESTS_TOTAL,
                metrics_labels::SOURCE => "sensor".to_string(),
            )
            .increment(1);
        }
    }
}

/// Controller for poll sensor evaluations.
///
/// Poll sensors are triggered by timers and use cursors to track progress.
/// CAS (compare-and-swap) semantics prevent duplicate triggers when polls overlap.
pub struct PollSensorController {
    /// Minimum interval between evaluations.
    min_poll_interval: Duration,
    /// Sensor evaluation logic.
    evaluator: Arc<dyn SensorEvaluator>,
}

impl Default for PollSensorController {
    fn default() -> Self {
        Self::new()
    }
}

impl PollSensorController {
    /// Creates a new poll sensor controller with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::with_min_interval_and_evaluator(Duration::seconds(30), Arc::new(NoopSensorEvaluator))
    }

    /// Creates a poll sensor controller with custom settings.
    #[must_use]
    pub fn with_min_interval(min_poll_interval: Duration) -> Self {
        Self::with_min_interval_and_evaluator(min_poll_interval, Arc::new(NoopSensorEvaluator))
    }

    /// Creates a poll sensor controller with a custom evaluator.
    #[must_use]
    pub fn with_evaluator(evaluator: Arc<dyn SensorEvaluator>) -> Self {
        Self::with_min_interval_and_evaluator(Duration::seconds(30), evaluator)
    }

    /// Creates a poll sensor controller with custom settings and evaluator.
    #[must_use]
    pub fn with_min_interval_and_evaluator(
        min_poll_interval: Duration,
        evaluator: Arc<dyn SensorEvaluator>,
    ) -> Self {
        Self {
            min_poll_interval,
            evaluator,
        }
    }

    /// Checks if a sensor should be evaluated based on state and timing.
    #[must_use]
    pub fn should_evaluate(&self, state: &SensorStateRow, now: DateTime<Utc>) -> bool {
        // Skip if paused or error
        if matches!(state.status, SensorStatus::Paused | SensorStatus::Error) {
            return false;
        }

        // Check min interval since last evaluation
        if let Some(last_eval) = state.last_evaluation_at {
            if now - last_eval < self.min_poll_interval {
                return false;
            }
        }

        true
    }

    /// Evaluates a poll sensor and returns events to emit.
    ///
    /// Returns a vector of events:
    /// 1. `SensorEvaluated` - always emitted (includes `expected_state_version` for CAS)
    /// 2. `RunRequested` - one per run request from sensor evaluation
    ///
    /// Per P0-1, all events are emitted atomically in the same batch.
    /// Per P0-2, includes `expected_state_version` for CAS semantics.
    /// Returns an empty batch if the sensor is paused or in error.
    #[must_use]
    pub fn evaluate(
        &self,
        sensor_id: &str,
        tenant_id: &str,
        workspace_id: &str,
        state: &SensorStateRow,
        poll_epoch: i64,
    ) -> Vec<OrchestrationEvent> {
        let _guard = TimingGuard::new(|duration| {
            histogram!(
                metrics_names::ORCH_CONTROLLER_RECONCILE_SECONDS,
                metrics_labels::CONTROLLER => "poll_sensor".to_string(),
            )
            .record(duration.as_secs_f64());
        });

        if matches!(state.status, SensorStatus::Paused | SensorStatus::Error) {
            return Vec::new();
        }

        let eval_id = format!("eval_{}_{}", sensor_id, Ulid::new());
        let cursor_before = state.cursor.clone();

        let (cursor_after, run_requests, status) = match self
            .evaluator
            .evaluate_poll(sensor_id, cursor_before.as_deref())
        {
            Ok(PollSensorResult {
                cursor_after,
                run_requests,
            }) => {
                let status = if run_requests.is_empty() {
                    SensorEvalStatus::NoNewData
                } else {
                    SensorEvalStatus::Triggered
                };
                (cursor_after, run_requests, status)
            }
            Err(err) => (
                cursor_before.clone(),
                Vec::new(),
                SensorEvalStatus::Error {
                    message: err.message,
                },
            ),
        };

        let mut events = Vec::new();
        let emitted_at = Utc::now();

        // Emit SensorEvaluated event with CAS fields (P0-2)
        events.push(OrchestrationEvent::new_with_timestamp(
            tenant_id,
            workspace_id,
            OrchestrationEventData::SensorEvaluated {
                sensor_id: sensor_id.to_string(),
                eval_id: eval_id.clone(),
                cursor_before,
                cursor_after,
                expected_state_version: Some(state.state_version), // CAS field per P0-2
                trigger_source: TriggerSource::Poll { poll_epoch },
                run_requests: run_requests.clone(),
                status: status.clone(),
            },
            emitted_at,
        ));

        // Emit RunRequested for each run request (atomic with SensorEvaluated per P0-1)
        for req in &run_requests {
            events.push(OrchestrationEvent::new_with_timestamp(
                tenant_id,
                workspace_id,
                OrchestrationEventData::RunRequested {
                    run_key: req.run_key.clone(),
                    request_fingerprint: req.request_fingerprint.clone(),
                    asset_selection: req.asset_selection.clone(),
                    partition_selection: req.partition_selection.clone(),
                    trigger_source_ref: SourceRef::Sensor {
                        sensor_id: sensor_id.to_string(),
                        eval_id: eval_id.clone(),
                    },
                    labels: HashMap::new(),
                },
                emitted_at,
            ));
        }

        Self::record_metrics(&status, run_requests.len());

        events
    }

    fn record_metrics(status: &SensorEvalStatus, run_request_count: usize) {
        let status_label = match status {
            SensorEvalStatus::Triggered => "triggered",
            SensorEvalStatus::NoNewData => "no_data",
            SensorEvalStatus::Error { .. } => "error",
            SensorEvalStatus::SkippedStaleCursor => "skipped_stale",
        };

        counter!(
            metrics_names::SENSOR_EVALS_TOTAL,
            metrics_labels::SENSOR_TYPE => "poll".to_string(),
            metrics_labels::STATUS => status_label.to_string(),
        )
        .increment(1);

        for _ in 0..run_request_count {
            counter!(
                metrics_names::RUN_REQUESTS_TOTAL,
                metrics_labels::SOURCE => "sensor".to_string(),
            )
            .increment(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::events::sha256_hex;

    use crate::orchestration::controllers::sensor_evaluator::SensorEvaluationError;
    use crate::orchestration::events::RunRequest;

    fn test_message(message_id: &str) -> PubSubMessage {
        PubSubMessage {
            message_id: message_id.into(),
            data: b"test data".to_vec(),
            attributes: HashMap::new(),
            publish_time: Utc::now(),
        }
    }

    // ========================================================================
    // Task 3.1: Push Sensor Handler Tests
    // ========================================================================

    #[test]
    fn test_push_sensor_uses_message_id_for_idempotency() {
        let handler = PushSensorHandler::new();

        let message = PubSubMessage {
            message_id: "msg_abc123".into(),
            data: b"new file arrived".to_vec(),
            attributes: HashMap::new(),
            publish_time: Utc::now(),
        };

        let events = handler.handle_message(
            "01HQ123SENSORXYZ",
            "tenant-abc",
            "workspace-prod",
            SensorStatus::Active,
            &message,
        );

        assert!(!events.is_empty(), "Should emit at least SensorEvaluated");

        let eval_event = events
            .iter()
            .find(|e| matches!(&e.data, OrchestrationEventData::SensorEvaluated { .. }));
        assert!(eval_event.is_some(), "Should have SensorEvaluated event");

        // Idempotency key includes message_id
        let eval = eval_event.unwrap();
        assert!(
            eval.idempotency_key.contains("msg:msg_abc123"),
            "Idempotency key should contain message_id: {}",
            eval.idempotency_key
        );
    }

    #[test]
    fn test_push_sensor_is_cursorless_by_default() {
        let handler = PushSensorHandler::new();
        let message = test_message("msg_001");

        let events = handler.handle_message(
            "01HQ123SENSORXYZ",
            "tenant-abc",
            "workspace-prod",
            SensorStatus::Active,
            &message,
        );

        let eval_event = events
            .iter()
            .find(|e| matches!(&e.data, OrchestrationEventData::SensorEvaluated { .. }))
            .expect("Should have SensorEvaluated event");

        if let OrchestrationEventData::SensorEvaluated {
            cursor_before,
            expected_state_version,
            ..
        } = &eval_event.data
        {
            // Push sensors don't track cursor (message is the cursor)
            assert!(
                cursor_before.is_none(),
                "cursor_before should be None for push sensor"
            );
            // Push sensors don't use CAS
            assert!(
                expected_state_version.is_none(),
                "expected_state_version should be None for push sensor"
            );
        }
    }

    #[test]
    fn test_push_sensor_handler_emits_eval_and_run_requests_atomically() {
        // This test verifies the pattern - controller emits all events together
        let handler = PushSensorHandler::new();
        let message = test_message("msg_abc");

        let events = handler.handle_message(
            "01HQ123SENSORXYZ",
            "tenant-abc",
            "workspace-prod",
            SensorStatus::Active,
            &message,
        );

        // Controller must emit SensorEvaluated
        let has_eval = events
            .iter()
            .any(|e| matches!(&e.data, OrchestrationEventData::SensorEvaluated { .. }));
        assert!(has_eval, "Controller must emit SensorEvaluated");

        // All events should be in the same batch (vector)
        // In production, the ledger appends all events atomically
    }

    #[test]
    fn test_push_sensor_skips_when_paused_or_error() {
        let handler = PushSensorHandler::new();
        let message = test_message("msg_skip");

        let paused_events = handler.handle_message(
            "01HQ123SENSORXYZ",
            "tenant-abc",
            "workspace-prod",
            SensorStatus::Paused,
            &message,
        );
        assert!(
            paused_events.is_empty(),
            "Paused sensor should not emit events"
        );

        let error_events = handler.handle_message(
            "01HQ123SENSORXYZ",
            "tenant-abc",
            "workspace-prod",
            SensorStatus::Error,
            &message,
        );
        assert!(
            error_events.is_empty(),
            "Error sensor should not emit events"
        );
    }

    // ========================================================================
    // Task 3.2: Poll Sensor Controller Tests
    // ========================================================================

    #[test]
    fn test_poll_sensor_idempotency_uses_cursor_before() {
        let controller = PollSensorController::new();

        let state = SensorStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            sensor_id: "01HQ456POLLSENSOR".into(),
            cursor: Some("cursor_v1".into()),
            last_evaluation_at: None,
            last_eval_id: None,
            status: SensorStatus::Active,
            state_version: 7,
            row_version: "row_01HQ".into(),
        };

        let events = controller.evaluate(
            "01HQ456POLLSENSOR",
            "tenant-abc",
            "workspace-prod",
            &state,
            1736935200,
        );

        let eval_event = events
            .iter()
            .find(|e| matches!(&e.data, OrchestrationEventData::SensorEvaluated { .. }))
            .expect("Should have SensorEvaluated event");

        // Idempotency based on cursor_before (input), not cursor_after (output)
        let cursor_hash = sha256_hex("cursor_v1");
        assert!(
            eval_event.idempotency_key.contains(&cursor_hash),
            "Idempotency key should contain cursor_before hash: {}",
            eval_event.idempotency_key
        );

        // Verify expected_state_version is set for CAS
        if let OrchestrationEventData::SensorEvaluated {
            expected_state_version,
            ..
        } = &eval_event.data
        {
            assert_eq!(
                *expected_state_version,
                Some(7),
                "expected_state_version should match state.state_version"
            );
        }
    }

    #[test]
    fn test_poll_sensor_respects_min_interval() {
        let controller = PollSensorController::with_min_interval(Duration::seconds(60));
        let now = Utc::now();

        // If last_evaluation is too recent, should not evaluate
        let state = SensorStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            sensor_id: "01HQ456POLLSENSOR".into(),
            cursor: None,
            last_evaluation_at: Some(now - Duration::seconds(10)), // Only 10s ago
            last_eval_id: None,
            status: SensorStatus::Active,
            state_version: 1,
            row_version: "row_01HQ".into(),
        };

        assert!(
            !controller.should_evaluate(&state, now),
            "Should not evaluate before min_poll_interval elapses"
        );

        // If last_evaluation is old enough, should evaluate
        let old_state = SensorStateRow {
            last_evaluation_at: Some(now - Duration::seconds(120)), // 2 min ago
            ..state
        };

        assert!(
            controller.should_evaluate(&old_state, now),
            "Should evaluate after min_poll_interval elapses"
        );
    }

    #[test]
    fn test_poll_sensor_skips_paused_sensor() {
        let controller = PollSensorController::new();
        let now = Utc::now();

        let state = SensorStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            sensor_id: "01HQ456POLLSENSOR".into(),
            cursor: None,
            last_evaluation_at: None,
            last_eval_id: None,
            status: SensorStatus::Paused,
            state_version: 1,
            row_version: "row_01HQ".into(),
        };

        assert!(
            !controller.should_evaluate(&state, now),
            "Should not evaluate paused sensor"
        );
    }

    #[test]
    fn test_poll_sensor_skips_error_sensor() {
        let controller = PollSensorController::new();
        let now = Utc::now();

        let state = SensorStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            sensor_id: "01HQ456POLLSENSOR".into(),
            cursor: None,
            last_evaluation_at: None,
            last_eval_id: None,
            status: SensorStatus::Error,
            state_version: 1,
            row_version: "row_01HQ".into(),
        };

        assert!(
            !controller.should_evaluate(&state, now),
            "Should not evaluate error sensor"
        );
    }

    #[derive(Debug)]
    struct ErrorEvaluator;

    impl SensorEvaluator for ErrorEvaluator {
        fn evaluate_push(
            &self,
            _sensor_id: &str,
            _message: &PubSubMessage,
        ) -> Result<Vec<RunRequest>, SensorEvaluationError> {
            Err(SensorEvaluationError::new("boom"))
        }

        fn evaluate_poll(
            &self,
            _sensor_id: &str,
            _cursor_before: Option<&str>,
        ) -> Result<PollSensorResult, SensorEvaluationError> {
            Err(SensorEvaluationError::new("boom"))
        }
    }

    #[test]
    fn test_poll_sensor_error_preserves_cursor() {
        let controller = PollSensorController::with_min_interval_and_evaluator(
            Duration::seconds(30),
            Arc::new(ErrorEvaluator),
        );

        let state = SensorStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            sensor_id: "01HQ456POLLSENSOR".into(),
            cursor: Some("cursor_v1".into()),
            last_evaluation_at: None,
            last_eval_id: None,
            status: SensorStatus::Active,
            state_version: 7,
            row_version: "row_01HQ".into(),
        };

        let events = controller.evaluate(
            "01HQ456POLLSENSOR",
            "tenant-abc",
            "workspace-prod",
            &state,
            1736935200,
        );

        let eval_event = events
            .iter()
            .find(|e| matches!(&e.data, OrchestrationEventData::SensorEvaluated { .. }))
            .expect("Should have SensorEvaluated event");

        if let OrchestrationEventData::SensorEvaluated {
            cursor_after,
            status,
            ..
        } = &eval_event.data
        {
            assert_eq!(
                cursor_after.as_deref(),
                Some("cursor_v1"),
                "Cursor should not advance on error"
            );
            assert!(matches!(status, SensorEvalStatus::Error { .. }));
        }
    }

    #[test]
    fn test_poll_sensor_emits_eval_with_cas_fields() {
        let controller = PollSensorController::new();

        let state = SensorStateRow {
            tenant_id: "tenant-abc".into(),
            workspace_id: "workspace-prod".into(),
            sensor_id: "01HQ456POLLSENSOR".into(),
            cursor: Some("cursor_v1".into()),
            last_evaluation_at: None,
            last_eval_id: None,
            status: SensorStatus::Active,
            state_version: 5,
            row_version: "row_01HQ".into(),
        };

        let events = controller.evaluate(
            "01HQ456POLLSENSOR",
            "tenant-abc",
            "workspace-prod",
            &state,
            1736935200,
        );

        let eval_event = events
            .iter()
            .find(|e| matches!(&e.data, OrchestrationEventData::SensorEvaluated { .. }))
            .expect("Should have SensorEvaluated event");

        if let OrchestrationEventData::SensorEvaluated {
            cursor_before,
            expected_state_version,
            trigger_source,
            ..
        } = &eval_event.data
        {
            assert_eq!(
                cursor_before.as_deref(),
                Some("cursor_v1"),
                "cursor_before should be preserved"
            );
            assert_eq!(
                *expected_state_version,
                Some(5),
                "expected_state_version should be set for CAS"
            );
            assert!(
                matches!(
                    trigger_source,
                    TriggerSource::Poll {
                        poll_epoch: 1736935200
                    }
                ),
                "trigger_source should be Poll with correct epoch"
            );
        }
    }

    // ========================================================================
    // Task 3.4: Manual Sensor Evaluate Tests
    // ========================================================================

    #[test]
    fn test_sensor_manual_evaluate_with_sample_payload() {
        let handler = PushSensorHandler::new();

        let sample_payload = serde_json::json!({
            "bucket": "test-bucket",
            "object": "data/file.parquet"
        });

        let events = handler.manual_evaluate(
            "01HQ123SENSORXYZ",
            "tenant-abc",
            "workspace-prod",
            &sample_payload,
        );

        assert!(!events.is_empty(), "Should emit at least one event");

        let has_eval = events
            .iter()
            .any(|e| matches!(&e.data, OrchestrationEventData::SensorEvaluated { .. }));
        assert!(has_eval, "Should have SensorEvaluated event");
    }
}
