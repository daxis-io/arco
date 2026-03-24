# Orchestration Output Visibility Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Update the orchestration protobuf contract to separate worker execution success from output visibility and codify the new `TaskOutput` visibility model.

**Architecture:** Keep the stable protobuf contract coarse: execution success remains on `TaskExecution.state`, while output consumability is represented on `TaskOutput` via `OutputVisibilityState`, `published_at`, and `publish_error`. Preserve wire compatibility where possible by keeping field numbers stable and changing stats fields to `optional int64`.

**Tech Stack:** Protobuf (`buf`, `prost`), Rust (`arco-proto` tests), existing repo docs under `docs/plans/`.

---

### Task 1: Add failing protobuf contract tests

**Files:**
- Modify: `crates/arco-proto/src/lib.rs`

**Step 1: Write the failing test**

- Add a test that expects generated `TaskOutput` to expose:
  - `row_count: Option<i64>`
  - `byte_size: Option<i64>`
  - `visibility_state`
  - `published_at`
  - `publish_error`
- Add a test that expects `OutputVisibilityState` enum values for `PENDING`, `VISIBLE`, and `FAILED`.

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-proto task_output_visibility_contract -- --nocapture`

Expected: compile/test failure because the generated protobuf types do not yet include the new fields or enum.

### Task 2: Update the protobuf schema

**Files:**
- Modify: `proto/arco/v1/orchestration.proto`

**Step 1: Add the minimal schema changes**

- Add `OutputVisibilityState`.
- Change `TaskOutput.row_count` and `TaskOutput.byte_size` to `optional int64`.
- Add `visibility_state`, `published_at`, and `publish_error` to `TaskOutput`.
- Update comments to reflect:
  - `TASK_STATE_SUCCEEDED` means execution success only.
  - `TaskOutput.files` are published/readable files.
  - `TASK_OPERATION_BACKFILL` is deprecated semantically.

**Step 2: Keep compatibility constraints**

- Preserve existing field numbers where possible.
- Use new field numbers for new fields.
- Keep `TASK_OPERATION_BACKFILL = 3` but mark it deprecated in docs/comments.

### Task 3: Make the tests pass

**Files:**
- Modify: `crates/arco-proto/src/lib.rs`
- Modify: `proto/arco/v1/orchestration.proto`

**Step 1: Run targeted tests**

Run: `cargo test -p arco-proto task_output_visibility_contract -- --nocapture`

Expected: PASS

**Step 2: Run the protobuf crate test suite**

Run: `cargo test -p arco-proto -- --nocapture`

Expected: PASS

### Task 4: Verify the contract docs stay aligned

**Files:**
- Reference: `docs/plans/2026-03-23-orchestration-output-visibility-design.md`

**Step 1: Check schema against design**

- Confirm enum names and field semantics match the design record.
- Confirm `TaskOutput` comments reflect the new public contract.

**Step 2: Final verification**

Run: `cargo test -p arco-proto -- --nocapture`

Expected: PASS with fresh output.
