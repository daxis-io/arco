//! Golden vector tests for plan fingerprint stability.
//!
//! Update the golden value only when intentionally changing fingerprint semantics
//! (and bump `PLAN_FINGERPRINT_VERSION` in `crates/arco-flow/src/plan.rs`).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::str::FromStr;

use arco_core::{AssetId, TaskId};
use arco_flow::plan::{AssetKey, PlanBuilder, ResourceRequirements, TaskSpec};
use arco_flow::task_key::TaskOperation;

#[test]
fn golden_single_task_fingerprint_v1() {
    // IDs are deterministic for reproducibility; fingerprint does not include them.
    let task_id = TaskId::from_str("01ARZ3NDEKTSV4RRFFQ69G5FAV").unwrap();
    let asset_id = AssetId::from_str("00000000-0000-0000-0000-000000000001").unwrap();

    let plan = PlanBuilder::new("acme", "production")
        .add_task(TaskSpec {
            task_id,
            asset_id,
            asset_key: AssetKey::new("raw", "events"),
            operation: TaskOperation::Materialize,
            partition_key: None,
            upstream_task_ids: vec![],
            stage: 0,
            priority: 0,
            resources: ResourceRequirements::default(),
        })
        .build()
        .unwrap();

    // If this fails and the change is intentional, bump the version and update this value.
    let expected = "sha256:6fb2d2adcd8cbe9af2dc86d711da22766ab78362cf6a20941a3f1b75c59af6a8";
    assert_eq!(plan.fingerprint, expected);
}
