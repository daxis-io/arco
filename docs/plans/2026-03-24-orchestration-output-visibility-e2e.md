# Orchestration Output Visibility E2E Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extend orchestration events, folded state, and API responses so output-producing tasks can move through `pending` / `visible` / `failed` output visibility states end-to-end without conflating those states with task execution.

**Architecture:** Keep controller-facing folded `RunState` semantics unchanged for this slice so existing reconciliation, cancellation, and rerun behavior remains stable. Add a distinct output-visibility lifecycle to the orchestration event stream and task/read model, persist it through Parquet, and derive the public/API run state from task execution plus required-output visibility.

**Tech Stack:** Rust, serde, chrono, Arrow/Parquet, axum, utoipa, cargo test

---

### Task 1: Add failing event/fold tests for output visibility transitions

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Modify: `crates/arco-flow/src/orchestration/events/mod.rs`

**Step 1: Write the failing test**

Add fold tests that prove:
- a materialize task can finish execution successfully and still remain output-visibility `Pending`
- a later visibility event can move that task to `Visible` and populate `published_at`
- a later visibility event can move that task to `Failed` and populate `publish_error`
- output visibility events are ignored for non-current attempts or unknown tasks

Add event serde/idempotency coverage for the new visibility event payload and enum.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow output_visibility -- --nocapture`
Expected: FAIL because the visibility event, enum, and fold logic do not exist yet.

**Step 3: Write minimal implementation**

Introduce the internal output-visibility event payload and task definition flag required for the tests to compile.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-flow output_visibility -- --nocapture`
Expected: PASS for the new fold/event tests.

**Step 5: Commit**

```bash
git add crates/arco-flow/src/orchestration/events/mod.rs crates/arco-flow/src/orchestration/compactor/fold.rs
git commit -m "flow: track task output visibility events"
```

### Task 2: Persist output visibility in folded rows and parquet round-trips

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/parquet_util.rs`

**Step 1: Write the failing test**

Add Parquet round-trip tests for `TaskRow` that preserve:
- `requires_visible_output`
- `output_visibility_state`
- `published_at`
- `publish_error`

Add merge/fold tests that confirm duplicate visibility events are idempotent and older row versions do not regress visibility state.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow parquet_util::tests::task_rows_round_trip_output_visibility -- --nocapture`
Expected: FAIL because the new columns are missing from schema/serialization.

**Step 3: Write minimal implementation**

Add the new task row fields, schema columns, Arrow writers/readers, and merge logic.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-flow parquet_util::tests::task_rows_round_trip_output_visibility -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-flow/src/orchestration/compactor/fold.rs crates/arco-flow/src/orchestration/compactor/parquet_util.rs
git commit -m "flow: persist task output visibility"
```

### Task 3: Mark which planned tasks require visible output

**Files:**
- Modify: `crates/arco-flow/src/orchestration/events/mod.rs`
- Modify: `crates/arco-flow/src/orchestration/selection.rs`
- Modify: `crates/arco-flow/src/orchestration/controllers/run_request_processor.rs`
- Modify: `crates/arco-flow/src/orchestration/controllers/run_bridge.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/service.rs`
- Modify: `crates/arco-api/src/routes/orchestration.rs`

**Step 1: Write the failing test**

Add planning tests that prove asset/materialize tasks are marked `requires_visible_output = true` while generic synthetic tasks and bridge fallbacks default to `false`.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow requires_visible_output -- --nocapture`
Expected: FAIL because `TaskDef` has no such field yet.

**Step 3: Write minimal implementation**

Extend `TaskDef`, all constructors, and the `PlanCreated -> TaskRow` fold path to carry the new flag.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-flow requires_visible_output -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-flow/src/orchestration/events/mod.rs crates/arco-flow/src/orchestration/selection.rs crates/arco-flow/src/orchestration/controllers/run_request_processor.rs crates/arco-flow/src/orchestration/controllers/run_bridge.rs crates/arco-flow/src/orchestration/compactor/service.rs crates/arco-api/src/routes/orchestration.rs
git commit -m "flow: mark tasks that require visible output"
```

### Task 4: Make public orchestration API visibility-aware

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`
- Modify: `crates/arco-api/src/openapi.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`

**Step 1: Write the failing test**

Add API route/unit tests that prove:
- a run with all task executions complete but a required output still `Pending` returns `Running`
- a run with a required output visibility `Failed` returns `Failed`
- a run returns `Succeeded` only when all required outputs are `Visible`
- task summaries expose output visibility details

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-api orchestration::tests::run_state_reflects_output_visibility -- --nocapture`
Expected: FAIL because public run state is derived only from folded run state today.

**Step 3: Write minimal implementation**

Derive API run state from folded run row plus related task rows. Preserve `Cancelling` behavior. Add task summary fields for visibility state, `published_at`, and `publish_error`.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-api orchestration::tests::run_state_reflects_output_visibility -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-api/src/routes/orchestration.rs crates/arco-api/src/openapi.rs crates/arco-flow/src/orchestration/compactor/fold.rs
git commit -m "api: expose output visibility-aware orchestration state"
```

### Task 5: Wire callback/runtime payloads and run verification

**Files:**
- Modify: `crates/arco-flow/src/orchestration/callbacks/types.rs`
- Modify: `crates/arco-flow/src/orchestration/callbacks/handlers.rs`
- Modify: `crates/arco-api/src/routes/tasks.rs`
- Modify: `docs/plans/2026-03-24-orchestration-output-visibility-e2e.md`

**Step 1: Write the failing test**

Add callback/request tests that prove the runtime-facing payloads can emit the new visibility fields and the handler records the correct events for initial `pending` visibility on successful output-producing tasks.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-flow handle_task_completed -- --nocapture`
Expected: FAIL because callback payloads do not yet model visibility state.

**Step 3: Write minimal implementation**

Extend callback/API task output payloads so the runtime can report initial visibility facts. Emit follow-on visibility events where the current system has enough information; keep explicit TODOs where a later publisher integration is still required.

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-flow handle_task_completed -- --nocapture`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-flow/src/orchestration/callbacks/types.rs crates/arco-flow/src/orchestration/callbacks/handlers.rs crates/arco-api/src/routes/tasks.rs docs/plans/2026-03-24-orchestration-output-visibility-e2e.md
git commit -m "flow: carry output visibility through callbacks"
```

### Task 6: Final verification

**Files:**
- Modify: `docs/plans/2026-03-24-orchestration-output-visibility-e2e.md`

**Step 1: Run focused verification**

Run:
- `cargo test -p arco-flow output_visibility -- --nocapture`
- `cargo test -p arco-api run_state_reflects_output_visibility -- --nocapture`

Expected: PASS.

**Step 2: Run broader verification**

Run:
- `cargo test -p arco-flow --lib`
- `cargo test -p arco-api orchestration -- --nocapture`

Expected: PASS.

**Step 3: Record verification notes**

Update the plan doc with any deviations, remaining TODOs, and exact commands executed.

**Step 4: Commit**

```bash
git add docs/plans/2026-03-24-orchestration-output-visibility-e2e.md
git commit -m "docs: record output visibility e2e verification"
```

---

## Execution Notes

### Implemented

- Added `OutputVisibilityState` and `TaskOutputVisibilityChanged` orchestration events.
- Persisted `requires_visible_output`, `output_visibility_state`, `published_at`, and `publish_error` in folded task rows and Parquet.
- Exposed task output visibility fields through the public orchestration and task callback APIs.
- Extended callback/runtime task output payloads so workers can report explicit visibility facts at completion time.
- Kept folded execution `RunState` semantics stable for controllers and compaction logic.

### Verified Deviations From Original Plan

- Successful required-output tasks no longer auto-enter visibility `Pending` on `TaskFinished`.
  - Reason: that behavior made public runs appear `RUNNING` indefinitely when no later publisher emitted `TaskOutputVisibilityChanged`.
  - Updated behavior: public run state only gates on explicit visibility events. Missing visibility continues to read as execution-successful.
- Bridge fallback tasks remain `requires_visible_output = false`.
  - This matches the acceptance criteria and avoids incorrectly gating synthetic fallback work on output publication.
- Public `completed_at` is now cleared whenever the derived public run state is non-terminal.
  - This avoids contradictory API responses like `state = RUNNING` with a terminal completion timestamp already present.
- Unknown persisted `output_visibility_state` values now fail fast during Parquet reads instead of silently degrading to `None`.
- Visibility fold handling is monotonic for terminal visibility states.
  - Once a task is `Visible` or `Failed`, later conflicting visibility events are ignored.

### Remaining Non-Goal

- This slice still does not introduce a broader standalone publisher/runtime component that emits `TaskOutputVisibilityChanged` after completion on its own.
- The model, storage, fold logic, and API now support `Pending` / `Visible` / `Failed` end-to-end, and explicit runtime/callback payloads can emit those states.
- A later upstream producer may still emit follow-up visibility events asynchronously after task completion.

### Verification

Commands run:

- `cargo fmt --all`
- `cargo test -p arco-flow test_handle_task_completed_emits_visibility_event_when_output_reports_visibility -- --nocapture`
- `cargo test -p arco-flow test_output_visibility_event_does_not_regress_visible_task_back_to_pending -- --nocapture`
- `cargo test -p arco-flow --test run_bridge_controller_tests run_bridge_fallback_task_does_not_require_visible_output -- --nocapture`
- `cargo test -p arco-flow test_read_tasks_rejects_unknown_output_visibility_state -- --nocapture`
- `cargo test -p arco-api test_map_run_row_state_treats_missing_visibility_as_succeeded_until_event_arrives -- --nocapture`
- `cargo test -p arco-api test_public_run_completed_at_clears_timestamp_for_non_terminal_public_state -- --nocapture`
- `cargo test -p arco-api test_task_output_conversion_preserves_delta_lineage -- --nocapture`
- `cargo test -p arco-flow --lib`
- `cargo test -p arco-flow --test run_bridge_controller_tests -- --nocapture`
- `cargo run -q -p arco-api --bin gen_openapi > "$tmp"` using a temporary file, then moved into `crates/arco-api/openapi.json`

Notes:

- Direct shell redirection into `crates/arco-api/openapi.json` was avoided after it proved unsafe during iteration; generation now writes to a temporary file first and only replaces the checked-in spec after validating non-empty output.
- Final API verification should include `cargo test -p arco-api -- --nocapture` and `cargo test -p arco-api --test openapi_contract -- --nocapture` after the regenerated OpenAPI is in place.
