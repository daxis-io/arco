//! Planner/runtime seam tests for the run-bridge compatibility compiler.

use std::fs;
use std::path::Path;

use arco_flow::application::{CurrentInProcessPlanningSnapshotProvider, PlanningSnapshotProvider};
use arco_flow::orchestration::events::OrchestrationEventData;
use arco_flow::planning::{CompileRequest, LogicalTime, PlanCompiler, RunIntent};

fn compile_run_bridge(
    correlation_id: &str,
    run_id: &str,
    plan_id: &str,
    asset_selection: Vec<String>,
    partition_selection: Option<Vec<String>>,
) -> arco_flow::planning::CompileResult {
    let intent = RunIntent::run_bridge_compatibility(asset_selection, partition_selection);
    let snapshot = CurrentInProcessPlanningSnapshotProvider::default().snapshot_for_intent(&intent);
    let compiler = PlanCompiler::for_run_bridge_compatibility();

    compiler
        .compile(CompileRequest {
            correlation_id: correlation_id.to_string(),
            run_id: run_id.to_string(),
            plan_id: plan_id.to_string(),
            intent,
            planning_snapshot_token: snapshot.token,
            logical_time: LogicalTime::new("source-row-version:evt-001"),
        })
        .expect("compile run bridge compatibility plan")
}

#[test]
fn same_compile_inputs_with_different_correlation_and_run_ids_keep_plan_fingerprint() {
    let first = compile_run_bridge(
        "corr-a",
        "run-a",
        "plan:run-a",
        vec![
            "analytics/users".to_string(),
            "analytics.orders".to_string(),
        ],
        Some(vec!["2026-06-28".to_string()]),
    );
    let second = compile_run_bridge(
        "corr-b",
        "run-b",
        "plan:run-b",
        vec![
            "analytics.orders".to_string(),
            "analytics/users".to_string(),
        ],
        Some(vec!["2026-06-28".to_string()]),
    );

    assert_eq!(first.plan_fingerprint, second.plan_fingerprint);
    assert_eq!(task_keys(&first.tasks), task_keys(&second.tasks));
    assert_eq!(
        task_asset_keys(&first.tasks),
        task_asset_keys(&second.tasks)
    );
    assert_eq!(
        task_partition_keys(&first.tasks),
        task_partition_keys(&second.tasks)
    );
    assert_eq!(first.diagnostics, second.diagnostics);
}

fn task_keys(tasks: &[arco_flow::orchestration::events::TaskDef]) -> Vec<&str> {
    tasks.iter().map(|task| task.key.as_str()).collect()
}

fn task_asset_keys(tasks: &[arco_flow::orchestration::events::TaskDef]) -> Vec<Option<&str>> {
    tasks.iter().map(|task| task.asset_key.as_deref()).collect()
}

fn task_partition_keys(tasks: &[arco_flow::orchestration::events::TaskDef]) -> Vec<Option<&str>> {
    tasks
        .iter()
        .map(|task| task.partition_key.as_deref())
        .collect()
}

#[test]
fn compatibility_plan_created_tasks_have_stable_keys_without_wire_shape_change() {
    let first = compile_run_bridge(
        "corr-a",
        "run-a",
        "plan:run-a",
        vec![
            "analytics/users".to_string(),
            "analytics.orders".to_string(),
        ],
        Some(vec!["2026-06-28".to_string()]),
    );
    let second = compile_run_bridge(
        "corr-b",
        "run-b",
        "plan:run-b",
        vec![
            "analytics.orders".to_string(),
            "analytics/users".to_string(),
        ],
        Some(vec!["2026-06-28".to_string()]),
    );

    let first_event = OrchestrationEventData::PlanCreated {
        run_id: "run-a".to_string(),
        plan_id: "plan:run-a".to_string(),
        tasks: first.tasks.clone(),
    };
    let second_event = OrchestrationEventData::PlanCreated {
        run_id: "run-b".to_string(),
        plan_id: "plan:run-b".to_string(),
        tasks: second.tasks.clone(),
    };

    let OrchestrationEventData::PlanCreated { tasks, .. } = first_event else {
        panic!("expected PlanCreated");
    };
    assert_eq!(
        tasks
            .iter()
            .map(|task| task.key.as_str())
            .collect::<Vec<_>>(),
        vec!["analytics.orders", "analytics.users"]
    );
    assert!(
        tasks
            .iter()
            .all(|task| task.partition_key.as_deref() == Some("2026-06-28"))
    );

    let OrchestrationEventData::PlanCreated {
        tasks: second_tasks,
        ..
    } = second_event
    else {
        panic!("expected PlanCreated");
    };
    assert_eq!(
        tasks
            .iter()
            .map(|task| task.key.as_str())
            .collect::<Vec<_>>(),
        second_tasks
            .iter()
            .map(|task| task.key.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        tasks
            .iter()
            .map(|task| task.asset_key.as_deref())
            .collect::<Vec<_>>(),
        second_tasks
            .iter()
            .map(|task| task.asset_key.as_deref())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        tasks
            .iter()
            .map(|task| task.partition_key.as_deref())
            .collect::<Vec<_>>(),
        second_tasks
            .iter()
            .map(|task| task.partition_key.as_deref())
            .collect::<Vec<_>>()
    );
    assert_eq!(first.plan_fingerprint, second.plan_fingerprint);
}

#[test]
fn invalid_run_bridge_selection_falls_back_to_materialize_task_with_diagnostic() {
    let result = compile_run_bridge(
        "corr-invalid",
        "run-invalid",
        "plan:run-invalid",
        vec!["invalid asset".to_string()],
        None,
    );

    assert_eq!(result.tasks.len(), 1);
    let task = result.tasks.first().expect("fallback task");
    assert_eq!(task.key, "materialize");
    assert_eq!(task.asset_key, None);
    assert_eq!(task.partition_key, None);
    assert!(!task.requires_visible_output);
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "run_bridge_invalid_asset_selection"),
        "expected invalid selection diagnostic, got {:?}",
        result.diagnostics
    );
}

#[test]
fn run_bridge_controller_does_not_import_planner_internals_or_selection_lowering() {
    let source_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/orchestration/controllers/run_bridge.rs");
    let source = fs::read_to_string(source_path).expect("read run bridge source");

    for forbidden in [
        "crate::planning",
        "PlanCompiler",
        "AssetGraph",
        "SelectionOptions",
        "build_task_defs_for_selection",
        "canonicalize_asset_key",
    ] {
        assert!(
            !source.contains(forbidden),
            "RunBridgeController must not contain planner-owned symbol: {forbidden}"
        );
    }
}
