//! Cross-crate contract tests.
//!
//! These tests validate that the contracts between arco-core, arco-catalog,
//! and arco-flow are correctly implemented and maintained.

use arco_core::{AssetId, RunId, TaskId};
use arco_flow::events::EventBuilder;
use arco_flow::plan::PlanBuilder;
use arco_flow::task::TaskState;
use arco_test_utils::{PlanFactory, TestContext};

/// Contract: Plan fingerprint is deterministic.
#[test]
fn contract_plan_fingerprint_is_deterministic() {
    let ctx = TestContext::new();

    // Create same plan twice with same IDs
    let task_a = TaskId::generate();
    let asset_a = AssetId::generate();

    let plan1 = PlanBuilder::new(&ctx.tenant_id, &ctx.workspace_id)
        .add_task(arco_flow::plan::TaskSpec {
            task_id: task_a,
            asset_id: asset_a,
            asset_key: arco_flow::plan::AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            priority: 0,
            stage: 0,
            resources: arco_flow::plan::ResourceRequirements::default(),
        })
        .build()
        .expect("plan1");

    let plan2 = PlanBuilder::new(&ctx.tenant_id, &ctx.workspace_id)
        .add_task(arco_flow::plan::TaskSpec {
            task_id: task_a,
            asset_id: asset_a,
            asset_key: arco_flow::plan::AssetKey::new("raw", "events"),
            partition_key: None,
            upstream_task_ids: vec![],
            priority: 0,
            stage: 0,
            resources: arco_flow::plan::ResourceRequirements::default(),
        })
        .build()
        .expect("plan2");

    assert_eq!(
        plan1.fingerprint, plan2.fingerprint,
        "same inputs should produce same fingerprint"
    );
}

/// Contract: Plan fingerprint survives serialization.
#[test]
fn contract_plan_fingerprint_survives_serialization() {
    let ctx = TestContext::new();
    let (plan, _, _, _) = PlanFactory::linear_dag(&ctx.tenant_id, &ctx.workspace_id);

    let json = serde_json::to_string(&plan).expect("serialize");
    let deserialized: arco_flow::plan::Plan = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(plan.fingerprint, deserialized.fingerprint);
}

/// Contract: IDs are URL-safe (alphanumeric only).
#[test]
fn contract_ids_are_url_safe() {
    let task_id = TaskId::generate();
    let asset_id = AssetId::generate();
    let run_id = RunId::generate();

    let ids = [
        task_id.to_string(),
        asset_id.to_string(),
        run_id.to_string(),
    ];

    for id in ids {
        assert!(
            id.chars().all(|c| c.is_ascii_alphanumeric()),
            "ID {id} contains non-URL-safe characters",
        );
        assert_eq!(id.len(), 26, "ULID should be 26 characters");
    }
}

/// Contract: Event envelope follows our CatalogEvent spec (per unified design).
///
/// The architecture defines a CatalogEvent envelope with these fields:
/// - event_id: ULID
/// - event_type: string
/// - event_version: u32 (for schema evolution)
/// - timestamp: DateTime<Utc>
/// - source: string
/// - tenant_id: string
/// - workspace_id: string
/// - idempotency_key: string (for dedupe during compaction)
/// - sequence_position: Option<u64> (monotonic position for watermarking)
/// - payload: Value (domain-specific data)
#[test]
fn contract_event_envelope_matches_architecture() {
    let run_id = RunId::generate();
    let task_id = TaskId::generate();

    let event = EventBuilder::task_started("tenant", "workspace", run_id, task_id, 1, "worker-1");

    // Architecture-mandated fields (CatalogEvent envelope)
    assert!(!event.id.is_empty(), "id is required");
    assert!(!event.event_type.is_empty(), "event_type is required");
    assert!(event.schema_version >= 1, "schema_version must be >= 1");
    assert!(!event.source.is_empty(), "source is required");
    assert!(!event.tenant_id.is_empty(), "tenant_id is required");
    assert!(!event.workspace_id.is_empty(), "workspace_id is required");
    assert!(
        !event.idempotency_key.is_empty(),
        "idempotency_key is required for dedupe"
    );

    // Verify id is a valid ULID (26 alphanumeric chars)
    assert_eq!(event.id.len(), 26);
    assert!(event.id.chars().all(|c| c.is_ascii_alphanumeric()));
}

/// Contract: idempotency_key enables compaction dedupe.
///
/// Architecture: "Compactor dedupes by idempotency_key during fold."
#[test]
fn contract_idempotency_key_is_stable_for_same_event() {
    let run_id = RunId::generate();
    let task_id = TaskId::generate();

    // Same logical event created twice
    let event1 = EventBuilder::task_started("tenant", "workspace", run_id, task_id, 1, "worker-1");
    let event2 = EventBuilder::task_started("tenant", "workspace", run_id, task_id, 1, "worker-1");

    // idempotency_key should be deterministic for the same logical event
    assert_eq!(
        event1.idempotency_key, event2.idempotency_key,
        "same logical event must have same idempotency_key"
    );
}

/// Contract: schema_version enables schema evolution.
///
/// Architecture: "Unknown schema_version should be rejected or routed to compatibility parser."
#[test]
fn contract_schema_version_enables_schema_evolution() {
    let run_id = RunId::generate();
    let task_id = TaskId::generate();

    let event = EventBuilder::task_started("tenant", "workspace", run_id, task_id, 1, "worker-1");

    // Current version should be 1
    assert_eq!(event.schema_version, 1, "current schema version is 1");

    // Serialize and verify version is in JSON
    let json = serde_json::to_value(&event).expect("serialize");
    assert_eq!(
        json.get("schemaVersion").and_then(|v| v.as_u64()),
        Some(1),
        "schemaVersion must be serialized"
    );
}

/// Contract: Events serialize to valid JSON.
#[test]
fn contract_events_serialize_to_json() {
    let run_id = RunId::generate();
    let task_id = TaskId::generate();

    let events = vec![
        EventBuilder::run_created("tenant", "workspace", run_id, "plan-1", "Manual", None),
        EventBuilder::run_started("tenant", "workspace", run_id, "plan-1"),
        EventBuilder::task_queued("tenant", "workspace", run_id, task_id, 1),
        EventBuilder::task_started("tenant", "workspace", run_id, task_id, 1, "worker-1"),
        EventBuilder::task_completed("tenant", "workspace", run_id, task_id, TaskState::Succeeded, 1),
        EventBuilder::run_completed(
            "tenant",
            "workspace",
            run_id,
            arco_flow::run::RunState::Succeeded,
            1,
            0,
            0,
            0,
        ),
    ];

    for event in events {
        let json = serde_json::to_string(&event);
        assert!(json.is_ok(), "Event should serialize: {event:?}");

        // Should deserialize back
        let parsed: arco_flow::events::EventEnvelope =
            serde_json::from_str(&json.expect("json")).expect("parse");
        assert_eq!(event.id, parsed.id);
    }
}

/// Contract: Plan stages reflect dependency depth.
#[test]
fn contract_plan_stages_reflect_dependency_depth() {
    let ctx = TestContext::new();
    let (plan, task_a, task_b, task_c) =
        PlanFactory::linear_dag(&ctx.tenant_id, &ctx.workspace_id);

    // Linear DAG: a (stage 0) -> b (stage 1) -> c (stage 2)
    let spec_a = plan.get_task(&task_a).expect("task a");
    let spec_b = plan.get_task(&task_b).expect("task b");
    let spec_c = plan.get_task(&task_c).expect("task c");

    assert_eq!(spec_a.stage, 0, "Root task should be stage 0");
    assert_eq!(spec_b.stage, 1, "Task depending on stage 0 should be stage 1");
    assert_eq!(spec_c.stage, 2, "Task depending on stage 1 should be stage 2");
}

/// Contract: Plan topological order respects dependencies.
#[test]
fn contract_plan_toposort_respects_dependencies() {
    let ctx = TestContext::new();
    let (plan, task_a, task_b, task_c) =
        PlanFactory::linear_dag(&ctx.tenant_id, &ctx.workspace_id);

    let positions: std::collections::HashMap<TaskId, usize> = plan
        .tasks
        .iter()
        .enumerate()
        .map(|(i, t)| (t.task_id, i))
        .collect();

    assert!(
        positions[&task_a] < positions[&task_b],
        "task_a must appear before task_b"
    );
    assert!(
        positions[&task_b] < positions[&task_c],
        "task_b must appear before task_c"
    );
}
