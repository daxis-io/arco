//! Property-based tests for arco-flow invariants.
//!
//! These tests use proptest to verify invariants hold across
//! randomly generated inputs.

#![cfg(feature = "test-utils")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{TimeZone, Utc};
use proptest::prelude::*;
use tokio_test::block_on;

use arco_core::{AssetId, FlowPaths, MemoryBackend, ScopedStorage, TaskId, WritePrecondition};
use arco_flow::orchestration::compactor::MicroCompactor;
use arco_flow::orchestration::events::{
    OrchestrationEvent, OrchestrationEventData, TaskDef, TaskOutcome, TriggerInfo,
};
use arco_flow::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
use arco_flow::run::RunState;
use arco_flow::task::TaskState;
use arco_flow::task_key::TaskOperation;

/// Generates a random TaskId (diverse, not constant per run).
fn arb_task_id() -> impl Strategy<Value = TaskId> {
    // Use any() to generate diverse IDs, not Just() which is constant per test run
    any::<u64>().prop_map(|_| TaskId::generate())
}

/// Generates a random AssetId (diverse, not constant per run).
fn arb_asset_id() -> impl Strategy<Value = AssetId> {
    any::<u64>().prop_map(|_| AssetId::generate())
}

/// Generates a random asset namespace.
fn arb_namespace() -> impl Strategy<Value = String> {
    prop::sample::select(vec!["raw", "staging", "mart", "gold"]).prop_map(String::from)
}

/// Generates a random asset name.
fn arb_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{2,20}".prop_map(|s| s)
}

/// Generates an arbitrary TaskSpec with varied properties.
fn arb_task_spec() -> impl Strategy<Value = TaskSpec> {
    (
        arb_task_id(),
        arb_asset_id(),
        arb_namespace(),
        arb_name(),
        prop::collection::vec(arb_task_id(), 0..3), // 0-3 dependencies
        0i32..100,                                  // priority
    )
        .prop_map(|(task_id, asset_id, ns, name, deps, priority)| TaskSpec {
            task_id,
            asset_id,
            asset_key: AssetKey::new(&ns, &name),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: deps,
            priority,
            stage: 0,
            resources: ResourceRequirements::default(),
        })
}

/// Generates an arbitrary event with varied properties.
fn arb_materialization_event() -> impl Strategy<Value = (String, String, i64, i64)> {
    (
        "[A-Z0-9]{26}",    // ULID-like materialization_id
        "[A-Z0-9]{26}",    // ULID-like asset_id
        0i64..1_000_000,   // row_count
        0i64..100_000_000, // byte_size
    )
}

fn orchestration_invariant_events() -> Vec<OrchestrationEvent> {
    let run_id = "run-prop-01";
    let plan_id = "plan-prop-01";
    let timestamp = Utc.with_ymd_and_hms(2025, 1, 15, 0, 0, 0).unwrap();

    let mut run_triggered = OrchestrationEvent::new_with_timestamp(
        "tenant",
        "workspace",
        OrchestrationEventData::RunTriggered {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            trigger: TriggerInfo::Manual {
                user_id: "tester@example.com".to_string(),
            },
            root_assets: vec!["analytics.daily".to_string()],
            run_key: None,
            labels: HashMap::new(),
            code_version: None,
        },
        timestamp,
    );
    run_triggered.event_id = "01HX0000000000000000000001".to_string();

    let mut plan_created = OrchestrationEvent::new_with_timestamp(
        "tenant",
        "workspace",
        OrchestrationEventData::PlanCreated {
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            tasks: vec![TaskDef {
                key: "extract".to_string(),
                depends_on: vec![],
                asset_key: None,
                partition_key: None,
                max_attempts: 3,
                heartbeat_timeout_sec: 60,
            }],
        },
        timestamp,
    );
    plan_created.event_id = "01HX0000000000000000000002".to_string();

    let mut task_started = OrchestrationEvent::new_with_timestamp(
        "tenant",
        "workspace",
        OrchestrationEventData::TaskStarted {
            run_id: run_id.to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            attempt_id: "attempt-01".to_string(),
            worker_id: "worker-1".to_string(),
        },
        timestamp,
    );
    task_started.event_id = "01HX0000000000000000000003".to_string();

    let mut task_finished = OrchestrationEvent::new_with_timestamp(
        "tenant",
        "workspace",
        OrchestrationEventData::TaskFinished {
            run_id: run_id.to_string(),
            task_key: "extract".to_string(),
            attempt: 1,
            attempt_id: "attempt-01".to_string(),
            worker_id: "worker-1".to_string(),
            outcome: TaskOutcome::Succeeded,
            materialization_id: None,
            error_message: None,
            output: None,
            error: None,
            metrics: None,
            cancelled_during_phase: None,
            partial_progress: None,
            asset_key: None,
            partition_key: None,
            code_version: None,
        },
        timestamp,
    );
    task_finished.event_id = "01HX0000000000000000000004".to_string();

    vec![run_triggered, plan_created, task_started, task_finished]
}

async fn compact_signature_for_path_order(path_order: &[usize]) -> (usize, usize, usize, usize) {
    let events = orchestration_invariant_events();
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage");

    let mut paths = Vec::with_capacity(events.len());
    for event in &events {
        let path = FlowPaths::orchestration_event_path("2025-01-15", &event.event_id);
        storage
            .put_raw(
                &path,
                Bytes::from(serde_json::to_string(event).expect("serialize")),
                WritePrecondition::None,
            )
            .await
            .expect("persist event");
        paths.push(path);
    }

    let ordered_paths: Vec<String> = path_order
        .iter()
        .map(|index| paths[*index].clone())
        .collect();

    MicroCompactor::new(storage.clone())
        .compact_events(ordered_paths)
        .await
        .expect("compact events");

    let (_, state) = MicroCompactor::new(storage)
        .load_state()
        .await
        .expect("load state");
    (
        state.runs.len(),
        state.tasks.len(),
        state.dep_satisfaction.len(),
        state.idempotency_keys.len(),
    )
}

async fn crash_replay_signature(split_index: usize) -> (usize, usize, usize, usize) {
    let events = orchestration_invariant_events();
    let backend = Arc::new(MemoryBackend::new());
    let storage = ScopedStorage::new(backend, "tenant", "workspace").expect("scoped storage");

    let mut paths = Vec::with_capacity(events.len());
    for event in &events {
        let path = FlowPaths::orchestration_event_path("2025-01-15", &event.event_id);
        storage
            .put_raw(
                &path,
                Bytes::from(serde_json::to_string(event).expect("serialize")),
                WritePrecondition::None,
            )
            .await
            .expect("persist event");
        paths.push(path);
    }

    let split = split_index.min(paths.len());
    let prefix = paths[..split].to_vec();
    let full_replay = paths.clone();

    if !prefix.is_empty() {
        MicroCompactor::new(storage.clone())
            .compact_events(prefix)
            .await
            .expect("prefix compact");
    }

    MicroCompactor::new(storage.clone())
        .compact_events(full_replay)
        .await
        .expect("full replay compact");

    let (_, state) = MicroCompactor::new(storage)
        .load_state()
        .await
        .expect("load state");
    (
        state.runs.len(),
        state.tasks.len(),
        state.dep_satisfaction.len(),
        state.idempotency_keys.len(),
    )
}

proptest! {
    /// INVARIANT: Plan fingerprint is deterministic for same inputs.
    #[test]
    fn plan_fingerprint_deterministic(
        tenant in "[a-z]{4,8}",
        workspace in "[a-z]{4,8}",
    ) {
        let task_id = TaskId::generate();
        let asset_id = AssetId::generate();

        let plan1 = PlanBuilder::new(&tenant, &workspace)
            .add_task(TaskSpec {
                task_id,
                asset_id,
                asset_key: AssetKey::new("raw", "events"),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: vec![],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        let plan2 = PlanBuilder::new(&tenant, &workspace)
            .add_task(TaskSpec {
                task_id,
                asset_id,
                asset_key: AssetKey::new("raw", "events"),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: vec![],
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            })
            .build()
            .unwrap();

        prop_assert_eq!(plan1.fingerprint, plan2.fingerprint);
    }

    /// INVARIANT: TaskState transitions are valid per actual state machine.
    ///
    /// State machine (from arco-flow/src/task.rs):
    /// - Planned -> Pending, Cancelled
    /// - Pending -> Ready, Skipped, Cancelled
    /// - Ready -> Queued, Cancelled
    /// - Queued -> Dispatched, Cancelled
    /// - Dispatched -> Running, Failed, Cancelled
    /// - Running -> Succeeded, Failed, Cancelled
    /// - Failed -> RetryWait, Cancelled
    /// - RetryWait -> Ready, Cancelled
    /// - Succeeded, Skipped, Cancelled -> (terminal)
    #[test]
    fn task_state_transitions_valid(
        initial in prop::sample::select(vec![
            TaskState::Planned,
            TaskState::Pending,
            TaskState::Ready,
            TaskState::Queued,
            TaskState::Dispatched,
            TaskState::Running,
        ]),
    ) {
        // From Planned, can go to Pending or Cancelled
        if initial == TaskState::Planned {
            prop_assert!(initial.can_transition_to(TaskState::Pending));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
            prop_assert!(!initial.can_transition_to(TaskState::Running));
        }

        // From Pending, can go to Ready, Skipped, or Cancelled
        if initial == TaskState::Pending {
            prop_assert!(initial.can_transition_to(TaskState::Ready));
            prop_assert!(initial.can_transition_to(TaskState::Skipped));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
            prop_assert!(!initial.can_transition_to(TaskState::Succeeded));
        }

        // From Ready, can go to Queued or Cancelled
        if initial == TaskState::Ready {
            prop_assert!(initial.can_transition_to(TaskState::Queued));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
        }

        // From Queued, can go to Dispatched or Cancelled
        if initial == TaskState::Queued {
            prop_assert!(initial.can_transition_to(TaskState::Dispatched));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
        }

        // From Dispatched, can go to Running, Failed, or Cancelled
        if initial == TaskState::Dispatched {
            prop_assert!(initial.can_transition_to(TaskState::Running));
            prop_assert!(initial.can_transition_to(TaskState::Failed));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
        }

        // From Running, can go to Succeeded, Failed, or Cancelled
        if initial == TaskState::Running {
            prop_assert!(initial.can_transition_to(TaskState::Succeeded));
            prop_assert!(initial.can_transition_to(TaskState::Failed));
            prop_assert!(initial.can_transition_to(TaskState::Cancelled));
        }

        // Terminal states cannot transition
        for terminal in [TaskState::Succeeded, TaskState::Failed, TaskState::Skipped, TaskState::Cancelled] {
            prop_assert!(!terminal.can_transition_to(TaskState::Pending));
            prop_assert!(!terminal.can_transition_to(TaskState::Queued));
            prop_assert!(!terminal.can_transition_to(TaskState::Running));
        }
    }

    /// INVARIANT: RunState transitions are valid per actual state machine.
    ///
    /// State machine (from arco-flow/src/run.rs):
    /// - Pending -> Running, Cancelling, Cancelled
    /// - Running -> Succeeded, Failed, Cancelling, TimedOut
    /// - Cancelling -> Cancelled
    /// - Succeeded, Failed, Cancelled, TimedOut -> (terminal)
    #[test]
    fn run_state_transitions_valid(
        initial in prop::sample::select(vec![
            RunState::Pending,
            RunState::Running,
            RunState::Cancelling,
        ]),
    ) {
        // From Pending, can go to Running, Cancelling, or Cancelled
        if initial == RunState::Pending {
            prop_assert!(initial.can_transition_to(RunState::Running));
            prop_assert!(initial.can_transition_to(RunState::Cancelling));
            prop_assert!(initial.can_transition_to(RunState::Cancelled));
            prop_assert!(!initial.can_transition_to(RunState::Succeeded));
        }

        // From Running, can go to Succeeded, Failed, Cancelling, or TimedOut
        if initial == RunState::Running {
            prop_assert!(initial.can_transition_to(RunState::Succeeded));
            prop_assert!(initial.can_transition_to(RunState::Failed));
            prop_assert!(initial.can_transition_to(RunState::Cancelling));
            prop_assert!(initial.can_transition_to(RunState::TimedOut));
            prop_assert!(!initial.can_transition_to(RunState::Cancelled)); // Must go via Cancelling
        }

        // From Cancelling, can only go to Cancelled
        if initial == RunState::Cancelling {
            prop_assert!(initial.can_transition_to(RunState::Cancelled));
            prop_assert!(!initial.can_transition_to(RunState::Succeeded));
        }

        // Terminal states cannot transition
        for terminal in [RunState::Succeeded, RunState::Failed, RunState::Cancelled, RunState::TimedOut] {
            prop_assert!(!terminal.can_transition_to(RunState::Pending));
            prop_assert!(!terminal.can_transition_to(RunState::Running));
        }
    }

    /// INVARIANT: Plan with N tasks has stages 0..max where max <= N-1.
    #[test]
    fn plan_stages_bounded(task_count in 1usize..10) {
        let mut builder = PlanBuilder::new("tenant", "workspace");
        let mut task_ids = Vec::new();

        // Create a linear chain
        for i in 0..task_count {
            let task_id = TaskId::generate();
            let upstream = if i > 0 { vec![task_ids[i - 1]] } else { vec![] };

            builder = builder.add_task(TaskSpec {
                task_id,
                asset_id: AssetId::generate(),
                asset_key: AssetKey::new("raw", format!("task_{i}")),
                operation: TaskOperation::Materialize,
                partition_key: None,
                upstream_task_ids: upstream,
                priority: 0,
                stage: 0,
                resources: ResourceRequirements::default(),
            });
            task_ids.push(task_id);
        }

        let plan = builder.build().unwrap();

        // Max stage should be task_count - 1 for a linear chain
        let max_stage = plan.tasks.iter().map(|t| t.stage).max().unwrap_or(0);
        prop_assert!(max_stage < task_count as u32);
    }

    /// INVARIANT: ULID strings are exactly 26 characters and alphanumeric.
    #[test]
    fn ulid_format_valid(_seed in 0u64..1000) {
        let task_id = TaskId::generate();
        let id_str = task_id.to_string();

        prop_assert_eq!(id_str.len(), 26);
        prop_assert!(id_str.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    /// INVARIANT: Any valid manifest produces a valid plan or valid rejection.
    /// Architecture 11.4: Plan generation is deterministic and validates inputs.
    #[test]
    fn plan_always_valid_or_valid_rejection(
        tasks in prop::collection::vec(arb_task_spec(), 1..10),
    ) {
        // Filter out self-referencing deps (invalid by definition)
        let valid_tasks: Vec<_> = tasks.into_iter()
            .map(|mut t| {
                t.upstream_task_ids.retain(|dep| *dep != t.task_id);
                t
            })
            .collect();

        let mut builder = PlanBuilder::new("tenant", "workspace");
        for task in valid_tasks {
            builder = builder.add_task(task);
        }

        let result = builder.build();

        match result {
            Ok(plan) => {
                // Plan has expected properties
                prop_assert!(!plan.fingerprint.is_empty());
                prop_assert!(!plan.tasks.is_empty());
            }
            Err(arco_flow::error::Error::CycleDetected { .. }) => {
                // Valid rejection - cycles are correctly detected
            }
            Err(arco_flow::error::Error::DependencyNotFound { .. }) => {
                // Valid rejection - missing deps correctly detected
            }
            Err(arco_flow::error::Error::PlanGenerationFailed { message })
                if message.contains("duplicate TaskKey") || message.contains("duplicate task_id") =>
            {
                // Valid rejection - input contains duplicate semantic tasks or duplicate IDs.
                // (These inputs are possible in the proptest generator and should be rejected.)
            }
            Err(e) => {
                prop_assert!(false, "Unexpected error: {e:?}");
            }
        }
    }

    /// INVARIANT: Event serialization is round-trip safe.
    #[test]
    fn event_serialization_roundtrip(
        event_data in arb_materialization_event(),
    ) {
        let (mat_id, asset_id, row_count, byte_size) = event_data;

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
        struct TestEvent {
            materialization_id: String,
            asset_id: String,
            row_count: i64,
            byte_size: i64,
        }

        let event = TestEvent {
            materialization_id: mat_id,
            asset_id,
            row_count,
            byte_size,
        };

        let json = serde_json::to_string(&event).expect("serialize");
        let parsed: TestEvent = serde_json::from_str(&json).expect("deserialize");

        prop_assert_eq!(event, parsed);
    }

    /// INVARIANT: ULID sorting yields non-decreasing timestamps.
    /// NOTE: ULIDs in the same millisecond are NOT guaranteed to maintain insertion order
    /// (the random component can reorder them). This test verifies the weaker but correct
    /// property: sorting ULIDs yields chronologically non-decreasing timestamps.
    #[test]
    fn ulid_sorting_yields_chronological_timestamps(count in 2usize..20) {
        let mut ids: Vec<ulid::Ulid> = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(ulid::Ulid::new());
        }

        // Sort ULIDs lexicographically
        let mut sorted = ids.clone();
        sorted.sort();

        // Verify: sorted ULIDs have non-decreasing timestamps
        for window in sorted.windows(2) {
            let ts1 = window[0].timestamp_ms();
            let ts2 = window[1].timestamp_ms();
            prop_assert!(
                ts1 <= ts2,
                "sorted ULIDs should have non-decreasing timestamps: {} > {}",
                ts1, ts2
            );
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn compaction_is_out_of_order_and_duplicate_invariant(
        mut replay_order in proptest::collection::vec(0usize..4, 4..12)
    ) {
        for index in 0..4 {
            if !replay_order.contains(&index) {
                replay_order.push(index);
            }
        }

        let canonical_order = vec![0, 1, 2, 3];
        let canonical = block_on(compact_signature_for_path_order(&canonical_order));
        let replayed = block_on(compact_signature_for_path_order(&replay_order));

        prop_assert_eq!(canonical, replayed);
    }

    #[test]
    fn compaction_crash_replay_converges_to_single_pass_state(split_index in 0usize..4) {
        let canonical = block_on(compact_signature_for_path_order(&[0, 1, 2, 3]));
        let recovered = block_on(crash_replay_signature(split_index));

        prop_assert_eq!(canonical, recovered);
    }
}
