//! Sensor semantics end-to-end (hermetic, CI-gated).
//!
//! This suite proves dagster-parity-07 sensor semantics across:
//! controller evaluation -> ledger events -> compactor fold -> durable projections.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use chrono::{DateTime, Duration, TimeZone, Utc};

use arco_core::{MemoryBackend, ScopedStorage};
use arco_flow::orchestration::LedgerWriter;
use arco_flow::orchestration::compactor::{FoldState, MicroCompactor};
use arco_flow::orchestration::controllers::sensor::{PollSensorController, PushSensorHandler};
use arco_flow::orchestration::controllers::sensor_evaluator::{
    PollSensorResult, PubSubMessage, SensorEvaluationError, SensorEvaluator,
};
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, RunRequest, SensorEvalStatus, SensorStatus,
    SourceRef, TriggerSource,
};
use arco_flow::orchestration::ids::run_id_from_run_key;

fn fixed_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("valid fixed timestamp")
}

fn normalize_events(events: &mut [OrchestrationEvent], prefix: &str, timestamp: DateTime<Utc>) {
    for (i, e) in events.iter_mut().enumerate() {
        e.event_id = format!("{prefix}_{i:02}");
        e.timestamp = timestamp;
    }
}

async fn append_and_compact(
    storage: &ScopedStorage,
    events: Vec<OrchestrationEvent>,
) -> arco_flow::error::Result<FoldState> {
    let ledger = LedgerWriter::new(storage.clone());
    let mut paths = Vec::new();

    for event in events {
        let path = LedgerWriter::event_path(&event);
        ledger.append(event).await?;
        paths.push(path);
    }

    MicroCompactor::new(storage.clone())
        .compact_events(paths)
        .await?;
    let (_, state) = MicroCompactor::new(storage.clone()).load_state().await?;
    Ok(state)
}

fn poll_seed_event(sensor_id: &str, cursor_after: &str, poll_epoch: i64) -> OrchestrationEvent {
    OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: sensor_id.to_string(),
            eval_id: format!("eval_{sensor_id}_seed"),
            cursor_before: None,
            cursor_after: Some(cursor_after.to_string()),
            expected_state_version: Some(0),
            trigger_source: TriggerSource::Poll { poll_epoch },
            run_requests: vec![],
            status: SensorEvalStatus::NoNewData,
        },
        fixed_time(),
    )
}

fn eval_id_from(events: &[OrchestrationEvent]) -> String {
    events
        .iter()
        .find_map(|e| match &e.data {
            OrchestrationEventData::SensorEvaluated { eval_id, .. } => Some(eval_id.clone()),
            _ => None,
        })
        .expect("expected SensorEvaluated in batch")
}

struct PushSingleRunEvaluator;

impl SensorEvaluator for PushSingleRunEvaluator {
    fn evaluate_push(
        &self,
        sensor_id: &str,
        message: &PubSubMessage,
    ) -> Result<Vec<RunRequest>, SensorEvaluationError> {
        Ok(vec![RunRequest {
            run_key: format!("sensor:{sensor_id}:msg:{}", message.message_id),
            request_fingerprint: format!("fp:msg:{}", message.message_id),
            asset_selection: vec!["analytics.summary".to_string()],
            partition_selection: None,
        }])
    }

    fn evaluate_poll(
        &self,
        _sensor_id: &str,
        cursor_before: Option<&str>,
    ) -> Result<PollSensorResult, SensorEvaluationError> {
        Ok(PollSensorResult {
            cursor_after: cursor_before.map(|c| c.to_string()),
            run_requests: Vec::new(),
        })
    }
}

struct CursorAdvancingPollEvaluator;

impl SensorEvaluator for CursorAdvancingPollEvaluator {
    fn evaluate_push(
        &self,
        _sensor_id: &str,
        _message: &PubSubMessage,
    ) -> Result<Vec<RunRequest>, SensorEvaluationError> {
        Ok(Vec::new())
    }

    fn evaluate_poll(
        &self,
        sensor_id: &str,
        cursor_before: Option<&str>,
    ) -> Result<PollSensorResult, SensorEvaluationError> {
        let cursor_after = match cursor_before {
            Some("cursor_v0") => Some("cursor_v1".to_string()),
            Some("cursor_v1") => Some("cursor_v2".to_string()),
            Some(other) => Some(format!("{other}_advanced")),
            None => Some("cursor_v0".to_string()),
        };

        let cursor_after_str = cursor_after.clone().unwrap_or_else(|| "none".to_string());

        Ok(PollSensorResult {
            cursor_after,
            run_requests: vec![RunRequest {
                run_key: format!("sensor:{sensor_id}:poll:{cursor_after_str}"),
                request_fingerprint: "fp:poll:v1".to_string(),
                asset_selection: vec!["analytics.summary".to_string()],
                partition_selection: None,
            }],
        })
    }
}

struct ConflictOnSecondCallPollEvaluator {
    call: AtomicU32,
}

impl ConflictOnSecondCallPollEvaluator {
    fn new() -> Self {
        Self {
            call: AtomicU32::new(0),
        }
    }
}

impl SensorEvaluator for ConflictOnSecondCallPollEvaluator {
    fn evaluate_push(
        &self,
        _sensor_id: &str,
        _message: &PubSubMessage,
    ) -> Result<Vec<RunRequest>, SensorEvaluationError> {
        Ok(Vec::new())
    }

    fn evaluate_poll(
        &self,
        sensor_id: &str,
        cursor_before: Option<&str>,
    ) -> Result<PollSensorResult, SensorEvaluationError> {
        let call = self.call.fetch_add(1, Ordering::SeqCst) + 1;

        let cursor_after = match cursor_before {
            Some("cursor_v0") => Some("cursor_v1".to_string()),
            Some("cursor_v1") => Some("cursor_v2".to_string()),
            Some(other) => Some(format!("{other}_advanced")),
            None => Some("cursor_v0".to_string()),
        };

        let cursor_after_str = cursor_after.as_deref().unwrap_or("none");
        let run_key = format!("sensor:{sensor_id}:poll:{cursor_after_str}");
        let request_fingerprint = format!("fp:poll:call:{call}");

        Ok(PollSensorResult {
            cursor_after,
            run_requests: vec![RunRequest {
                run_key,
                request_fingerprint,
                asset_selection: vec!["analytics.summary".to_string()],
                partition_selection: None,
            }],
        })
    }
}

#[derive(Debug)]
struct ErrorPollEvaluator;

impl SensorEvaluator for ErrorPollEvaluator {
    fn evaluate_push(
        &self,
        _sensor_id: &str,
        _message: &PubSubMessage,
    ) -> Result<Vec<RunRequest>, SensorEvaluationError> {
        Ok(Vec::new())
    }

    fn evaluate_poll(
        &self,
        _sensor_id: &str,
        _cursor_before: Option<&str>,
    ) -> Result<PollSensorResult, SensorEvaluationError> {
        Err(SensorEvaluationError::new("boom"))
    }
}

#[tokio::test]
async fn push_duplicate_message_id_delivery_is_idempotent_end_to_end()
-> arco_flow::error::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let sensor_id = "01HQ123SENSORXYZ";
    let handler = PushSensorHandler::with_evaluator(Arc::new(PushSingleRunEvaluator));

    let message = PubSubMessage {
        message_id: "msg_001".into(),
        data: b"payload".to_vec(),
        attributes: HashMap::new(),
        publish_time: fixed_time(),
    };

    let mut batch1 = handler.handle_message(
        sensor_id,
        "tenant-abc",
        "workspace-prod",
        SensorStatus::Active,
        &message,
    );
    normalize_events(&mut batch1, "evt_01_push", fixed_time());

    let state1 = append_and_compact(&storage, batch1).await?;

    let sensor_state = state1.sensor_state.get(sensor_id).expect("sensor state");
    assert_eq!(sensor_state.cursor.as_deref(), Some("msg_001"));
    assert_eq!(sensor_state.state_version, 1);

    assert_eq!(state1.sensor_evals.len(), 1, "one durable evaluation");

    let expected_run_key = format!("sensor:{sensor_id}:msg:msg_001");
    let expected_run_id =
        run_id_from_run_key("tenant-abc", "workspace-prod", &expected_run_key, &[]);

    let run_row = state1
        .run_key_index
        .get(expected_run_key.as_str())
        .expect("expected run_key_index row");

    assert_eq!(run_row.run_id, expected_run_id);

    let mut batch2 = handler.handle_message(
        sensor_id,
        "tenant-abc",
        "workspace-prod",
        SensorStatus::Active,
        &message,
    );
    normalize_events(
        &mut batch2,
        "evt_02_push_dup",
        fixed_time() + Duration::seconds(1),
    );

    let state2 = append_and_compact(&storage, batch2).await?;

    assert_eq!(state2.sensor_state.get(sensor_id).unwrap().state_version, 1);
    assert_eq!(state2.sensor_evals.len(), 1, "no duplicate durable evals");
    assert_eq!(state2.run_key_index.len(), 1, "no duplicate runs");

    Ok(())
}

#[tokio::test]
async fn poll_cas_mismatch_noops_cursor_and_drops_run_requested_end_to_end()
-> arco_flow::error::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let sensor_id = "01HQ456POLLSENSOR";

    // Seed durable state with cursor_v0.
    let mut seed = vec![poll_seed_event(sensor_id, "cursor_v0", 100)];
    normalize_events(&mut seed, "evt_00_seed", fixed_time());
    let seeded = append_and_compact(&storage, seed).await?;

    let seeded_state = seeded
        .sensor_state
        .get(sensor_id)
        .expect("seeded sensor state");
    assert_eq!(seeded_state.cursor.as_deref(), Some("cursor_v0"));
    assert_eq!(seeded_state.state_version, 1);

    let evaluator = Arc::new(ConflictOnSecondCallPollEvaluator::new());
    let controller =
        PollSensorController::with_min_interval_and_evaluator(Duration::seconds(30), evaluator);

    // First evaluation (CAS matches) updates cursor and registers run request.
    let mut batch1 =
        controller.evaluate(sensor_id, "tenant-abc", "workspace-prod", seeded_state, 101);
    normalize_events(
        &mut batch1,
        "evt_01_poll",
        fixed_time() + Duration::seconds(10),
    );

    let state1 = append_and_compact(&storage, batch1).await?;

    let state_row_1 = state1.sensor_state.get(sensor_id).expect("sensor state");
    assert_eq!(state_row_1.cursor.as_deref(), Some("cursor_v1"));
    assert_eq!(state_row_1.state_version, 2);

    assert_eq!(state1.run_key_index.len(), 1);
    assert!(state1.run_key_conflicts.is_empty());

    // Second evaluation uses the stale pre-eval state snapshot, generating stale expected_state_version.
    // Its RunRequested must be dropped (no conflict recorded).
    let mut batch2 =
        controller.evaluate(sensor_id, "tenant-abc", "workspace-prod", seeded_state, 102);
    let stale_eval_id = eval_id_from(&batch2);
    normalize_events(
        &mut batch2,
        "evt_02_poll_stale",
        fixed_time() + Duration::seconds(20),
    );

    let state2 = append_and_compact(&storage, batch2).await?;

    let state_row_2 = state2.sensor_state.get(sensor_id).expect("sensor state");
    assert_eq!(state_row_2.cursor.as_deref(), Some("cursor_v1"));
    assert_eq!(state_row_2.state_version, 2);

    let stale_eval = state2
        .sensor_evals
        .get(stale_eval_id.as_str())
        .expect("stale eval row should be recorded");

    assert!(matches!(
        stale_eval.status,
        SensorEvalStatus::SkippedStaleCursor
    ));
    assert_eq!(state2.run_key_index.len(), 1);
    assert!(
        state2.run_key_conflicts.is_empty(),
        "stale run requests must not create conflicts"
    );

    Ok(())
}

#[tokio::test]
async fn poll_cursor_is_durable_across_multiple_folds_and_runs_are_deterministic()
-> arco_flow::error::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let sensor_id = "01HQ789POLLSENSOR";

    let mut seed = vec![poll_seed_event(sensor_id, "cursor_v0", 200)];
    normalize_events(&mut seed, "evt_00_seed", fixed_time());
    append_and_compact(&storage, seed).await?;

    let (_, state0) = MicroCompactor::new(storage.clone()).load_state().await?;
    let state_row_0 = state0.sensor_state.get(sensor_id).expect("seeded state");

    let controller = PollSensorController::with_min_interval_and_evaluator(
        Duration::seconds(1),
        Arc::new(CursorAdvancingPollEvaluator),
    );

    let mut batch1 =
        controller.evaluate(sensor_id, "tenant-abc", "workspace-prod", state_row_0, 201);
    normalize_events(
        &mut batch1,
        "evt_01_poll",
        fixed_time() + Duration::seconds(10),
    );
    let state1 = append_and_compact(&storage, batch1).await?;

    let row1 = state1
        .sensor_state
        .get(sensor_id)
        .expect("state after poll1");
    assert_eq!(row1.cursor.as_deref(), Some("cursor_v1"));
    assert_eq!(row1.state_version, 2);

    let run_key_1 = format!("sensor:{sensor_id}:poll:cursor_v1");
    let run_id_1 = run_id_from_run_key("tenant-abc", "workspace-prod", &run_key_1, &[]);
    assert_eq!(
        state1.run_key_index.get(run_key_1.as_str()).unwrap().run_id,
        run_id_1
    );

    let (_, state_reloaded) = MicroCompactor::new(storage.clone()).load_state().await?;
    let row_reloaded = state_reloaded
        .sensor_state
        .get(sensor_id)
        .expect("reloaded state");

    let mut batch2 =
        controller.evaluate(sensor_id, "tenant-abc", "workspace-prod", row_reloaded, 202);
    normalize_events(
        &mut batch2,
        "evt_02_poll",
        fixed_time() + Duration::seconds(20),
    );
    let state2 = append_and_compact(&storage, batch2).await?;

    let row2 = state2
        .sensor_state
        .get(sensor_id)
        .expect("state after poll2");
    assert_eq!(row2.cursor.as_deref(), Some("cursor_v2"));
    assert_eq!(row2.state_version, 3);

    let run_key_2 = format!("sensor:{sensor_id}:poll:cursor_v2");
    let run_id_2 = run_id_from_run_key("tenant-abc", "workspace-prod", &run_key_2, &[]);
    assert_eq!(
        state2.run_key_index.get(run_key_2.as_str()).unwrap().run_id,
        run_id_2
    );

    Ok(())
}

#[tokio::test]
async fn poll_error_preserves_cursor_and_min_interval_backoff_is_deterministic()
-> arco_flow::error::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let sensor_id = "01HQ000ERRORPOLL";

    let mut seed = vec![poll_seed_event(sensor_id, "cursor_v1", 300)];
    normalize_events(&mut seed, "evt_00_seed", fixed_time());
    let seeded = append_and_compact(&storage, seed).await?;

    let seeded_row = seeded.sensor_state.get(sensor_id).expect("seeded state");

    let controller = PollSensorController::with_min_interval_and_evaluator(
        Duration::seconds(30),
        Arc::new(ErrorPollEvaluator),
    );

    let eval_time = fixed_time() + Duration::seconds(10);
    let mut batch = controller.evaluate(sensor_id, "tenant-abc", "workspace-prod", seeded_row, 301);
    normalize_events(&mut batch, "evt_01_error", eval_time);

    let state = append_and_compact(&storage, batch).await?;
    let row = state.sensor_state.get(sensor_id).expect("sensor state");

    assert_eq!(row.cursor.as_deref(), Some("cursor_v1"));
    assert_eq!(row.status, SensorStatus::Active);
    assert!(row.last_evaluation_at.is_some());

    assert!(!controller.should_evaluate(row, eval_time + Duration::seconds(10)));
    assert!(controller.should_evaluate(row, eval_time + Duration::seconds(31)));

    Ok(())
}

#[tokio::test]
async fn run_key_idempotency_is_enforced_even_without_event_idempotency()
-> arco_flow::error::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let sensor_id = "01HQRUNKEYSENSOR";
    let eval_id = "eval_run_key";

    let mut eval = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: sensor_id.to_string(),
            eval_id: eval_id.to_string(),
            cursor_before: None,
            cursor_after: Some("cursor_v1".to_string()),
            expected_state_version: None,
            trigger_source: TriggerSource::Push {
                message_id: "msg_run_key".into(),
            },
            run_requests: vec![],
            status: SensorEvalStatus::Triggered,
        },
        fixed_time(),
    );
    eval.event_id = "evt_00_eval".to_string();

    let run_key = "sensor:run_key:stable";
    let request_fingerprint = "fp_v1";

    let source = SourceRef::Sensor {
        sensor_id: sensor_id.to_string(),
        eval_id: eval_id.to_string(),
    };

    let mut rr1 = OrchestrationEvent::new_with_timestamp_and_idempotency_key(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: run_key.to_string(),
            request_fingerprint: request_fingerprint.to_string(),
            asset_selection: vec!["analytics.summary".to_string()],
            partition_selection: None,
            trigger_source_ref: source.clone(),
            labels: HashMap::new(),
        },
        "runreq_override:01",
        fixed_time() + Duration::seconds(1),
    );
    rr1.event_id = "evt_01_rr1".to_string();

    let mut rr2 = OrchestrationEvent::new_with_timestamp_and_idempotency_key(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: run_key.to_string(),
            request_fingerprint: request_fingerprint.to_string(),
            asset_selection: vec!["analytics.summary".to_string()],
            partition_selection: None,
            trigger_source_ref: source,
            labels: HashMap::new(),
        },
        "runreq_override:02",
        fixed_time() + Duration::seconds(2),
    );
    rr2.event_id = "evt_02_rr2".to_string();

    let state = append_and_compact(&storage, vec![eval, rr1, rr2]).await?;

    assert_eq!(state.run_key_index.len(), 1);
    assert!(state.run_key_conflicts.is_empty());

    let expected_run_id = run_id_from_run_key("tenant-abc", "workspace-prod", run_key, &[]);
    assert_eq!(
        state
            .run_key_index
            .get(run_key)
            .expect("run key index")
            .run_id,
        expected_run_id
    );

    Ok(())
}

#[tokio::test]
async fn run_key_conflict_is_durable_across_compactor_reload() -> arco_flow::error::Result<()> {
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace")?;

    let sensor_id = "01HQCONFLICTSENSOR";
    let eval_id = "eval_run_key_conflict";

    let mut eval = OrchestrationEvent::new_with_timestamp(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::SensorEvaluated {
            sensor_id: sensor_id.to_string(),
            eval_id: eval_id.to_string(),
            cursor_before: None,
            cursor_after: Some("cursor_v1".to_string()),
            expected_state_version: None,
            trigger_source: TriggerSource::Push {
                message_id: "msg_conflict".into(),
            },
            run_requests: vec![],
            status: SensorEvalStatus::Triggered,
        },
        fixed_time(),
    );
    eval.event_id = "evt_00_eval".to_string();

    let run_key = "sensor:run_key:conflict:stable";

    let source = SourceRef::Sensor {
        sensor_id: sensor_id.to_string(),
        eval_id: eval_id.to_string(),
    };

    let mut rr1 = OrchestrationEvent::new_with_timestamp_and_idempotency_key(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: run_key.to_string(),
            request_fingerprint: "fp_a".to_string(),
            asset_selection: vec!["analytics.summary".to_string()],
            partition_selection: None,
            trigger_source_ref: source.clone(),
            labels: HashMap::new(),
        },
        "runreq_override:conflict:01",
        fixed_time() + Duration::seconds(1),
    );
    rr1.event_id = "evt_01_rr1".to_string();

    let mut rr2 = OrchestrationEvent::new_with_timestamp_and_idempotency_key(
        "tenant-abc",
        "workspace-prod",
        OrchestrationEventData::RunRequested {
            run_key: run_key.to_string(),
            request_fingerprint: "fp_b".to_string(),
            asset_selection: vec!["analytics.summary".to_string()],
            partition_selection: None,
            trigger_source_ref: source,
            labels: HashMap::new(),
        },
        "runreq_override:conflict:02",
        fixed_time() + Duration::seconds(2),
    );
    rr2.event_id = "evt_02_rr2".to_string();

    let state = append_and_compact(&storage, vec![eval, rr1, rr2]).await?;

    assert_eq!(state.run_key_index.len(), 1);
    assert_eq!(state.run_key_conflicts.len(), 1);

    let run_row = state
        .run_key_index
        .get(run_key)
        .expect("expected run_key_index row");
    assert_eq!(run_row.request_fingerprint, "fp_a");
    assert_eq!(
        run_row.run_id,
        run_id_from_run_key("tenant-abc", "workspace-prod", run_key, &[])
    );

    let expected_conflict_id = format!("conflict:{run_key}:evt_02_rr2");
    let conflict = state
        .run_key_conflicts
        .get(expected_conflict_id.as_str())
        .expect("expected conflict row");
    assert_eq!(conflict.existing_fingerprint, "fp_a");
    assert_eq!(conflict.conflicting_fingerprint, "fp_b");
    assert_eq!(conflict.conflicting_event_id, "evt_02_rr2");
    assert_eq!(conflict.detected_at, fixed_time() + Duration::seconds(2));

    let (_, reloaded) = MicroCompactor::new(storage.clone()).load_state().await?;
    assert!(
        reloaded
            .run_key_conflicts
            .contains_key(expected_conflict_id.as_str())
    );

    Ok(())
}
