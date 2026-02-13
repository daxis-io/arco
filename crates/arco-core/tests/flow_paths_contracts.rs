//! Contract tests for canonical flow/orchestration path builders.

use arco_core::flow_paths::FlowPaths;

#[test]
fn orchestration_and_flow_paths_are_stable() {
    assert_eq!(
        FlowPaths::orchestration_event_path("2026-02-12", "01ABC"),
        "ledger/orchestration/2026-02-12/01ABC.json"
    );
    assert_eq!(
        FlowPaths::flow_event_path("executions", "2026-02-12", "01ABC"),
        "ledger/flow/executions/2026-02-12/01ABC.json"
    );
    assert_eq!(
        FlowPaths::orchestration_manifest_path(),
        "state/orchestration/manifest.json"
    );
    assert_eq!(
        FlowPaths::orchestration_l0_dir("delta-001"),
        "state/orchestration/l0/delta-001"
    );
}
