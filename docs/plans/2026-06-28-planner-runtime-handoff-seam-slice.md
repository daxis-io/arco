# Phase 1B Planner/Runtime Handoff Seam Slice

## Goal

Introduce the planner/runtime handoff seam without changing durable orchestration event shape. This slice moves the `RunBridgeController` compatibility task-lowering path behind planner-owned code while preserving:

- `OrchestrationEventData::PlanCreated { run_id, plan_id, tasks }`
- `proto/arco/orchestration/v1/orchestration.proto`
- existing API and control-store authority boundaries

`RunRequestProcessor` stays unchanged in this slice because its task-building behavior differs from `run_bridge`; migrating it is the next child slice.

## Source Documents

These roadmap/design docs were read as local source inputs for this slice. This
PR-prep branch packages the Phase 1B seam, not the broader dirty roadmap/design
baseline; source docs that are not already on `origin/main` are intentionally
not folded into this slice.

- `docs/plans/2026-06-27-arco-unified-execution-roadmap.md`
- `docs/plans/2026-06-27-planner-runtime-seam-hardening-design.md`
- `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/plans/2026-06-25-arco-tier1-control-store-strategy.md`
- `docs/plans/2026-06-20-olympia-inspired-arco-strategy.md`
- `docs/plans/2026-06-26-lineage-observation-projection-design.md`
- `docs/guide/src/reference/control-plane-scope.md`

## Implementation Scope

- Add `crates/arco-flow/src/planning/`:
  - `compiler.rs`
  - `diagnostics.rs`
  - `fingerprint.rs`
  - `selection.rs`
- Move selection semantics into `planning::selection`.
- Keep `orchestration::selection` as a compatibility re-export.
- Add `crates/arco-flow/src/application/`:
  - `planning_snapshot_provider.rs`
  - `run_planner.rs`
- Publish `pub mod planning;` and `pub mod application;`.
- Route `RunBridgeController` through application-level `RunPlanner`.

## Non-Goals

- Do not change `PlanCreated` durable event fields.
- Do not change protobuf shape or HTTP API behavior.
- Do not migrate `RunRequestProcessor` in this slice.
- Do not make runtime controller code import `planning::*` or call `PlanCompiler`.
- Do not introduce new control-store authority or durable plan-artifact storage.

## Fingerprint Contract

`PlanFingerprint` material includes:

- normalized run intent;
- named planning snapshot token;
- compiler version;
- explicit logical time;
- canonical task definitions.

It excludes:

- `run_id`;
- `plan_id`;
- event id;
- worker identity;
- queue state;
- runtime capacity;
- attempt identity;
- wall-clock compilation time.

## Tests And Verification

Add `crates/arco-flow/tests/planning_compiler_tests.rs` covering:

- same compile inputs with different correlation/run ids produce the same `PlanFingerprint`;
- compatibility `PlanCreated { tasks }` maps to stable task keys and same fingerprint when run id / plan id differ;
- invalid `run_bridge` compatibility selection preserves fallback `materialize` task behavior and emits a diagnostic;
- `RunBridgeController` source does not import planner internals or task-lowering helpers.

Run:

```bash
cargo test -p arco-flow --test planning_compiler_tests
cargo test -p arco-flow --test orchestration_selection_tests
cargo test -p arco-flow --test run_bridge_controller_tests
cargo test -p arco-flow --test orchestration_runtime_e2e_tests
cargo test -p arco-flow run_bridge
cargo fmt --check
git diff --check
```

## Exit Gate

Exit when `RunBridgeController` no longer performs asset-selection-to-`TaskDef` lowering, planner/application interfaces exist with deterministic fingerprint tests, `PlanCreated` wire compatibility is unchanged, and the next slice is explicitly documented as `RunRequestProcessor` migration.
