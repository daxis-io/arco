//! Integration tests for sensor orchestration behavior.

use arco_flow::orchestration::compactor::FoldState;
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, SensorEvalStatus, TriggerSource,
};
use ulid::Ulid;

fn sensor_eval_event(sensor_id: &str, message_id: &str) -> OrchestrationEvent {
    OrchestrationEvent::new(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: sensor_id.to_string(),
            eval_id: format!("eval_{}", Ulid::new()),
            cursor_before: None,
            cursor_after: Some(message_id.to_string()),
            expected_state_version: None,
            trigger_source: TriggerSource::Push {
                message_id: message_id.to_string(),
            },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
    )
}

#[test]
fn test_sensor_eval_idempotency_dedupes_duplicate_message() {
    let mut state = FoldState::new();

    let event1 = sensor_eval_event("01HQ123SENSORXYZ", "msg_001");
    let event2 = sensor_eval_event("01HQ123SENSORXYZ", "msg_001");

    state.fold_event(&event1);
    state.fold_event(&event2);

    let sensor_state = state.sensor_state.get("01HQ123SENSORXYZ").unwrap();
    assert_eq!(
        sensor_state.state_version, 1,
        "duplicate sensor eval should not advance state"
    );
    assert_eq!(state.sensor_evals.len(), 1);
    assert_eq!(state.idempotency_keys.len(), 1);
}
