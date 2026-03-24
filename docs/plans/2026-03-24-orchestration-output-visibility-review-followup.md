# Orchestration Output Visibility Review Follow-up Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Address the review feedback on the orchestration output-visibility contract slice by making the contract self-consistent, validated, and better tested without pulling in the broader runtime/read-model follow-up.

**Architecture:** Keep this branch scoped to the protobuf contract crate and schema comments. Add explicit validation helpers in `arco-proto` for `TaskOutput` invariants, strengthen encode/decode tests so optional stats have real regression coverage, and revise proto comments so they describe the contract fields accurately without claiming end-to-end runtime behavior that is not implemented in this branch.

**Tech Stack:** Protobuf (`buf`, `prost`), Rust (`arco-proto`), repo docs under `docs/plans/`.

---

### Task 1: Add failing contract tests

**Files:**
- Modify: `crates/arco-proto/src/lib.rs`

**Step 1: Write failing validation tests**

- Add tests for:
  - rejecting `TaskOutput` with `visibility_state == OUTPUT_VISIBILITY_STATE_UNSPECIFIED`
  - rejecting `VISIBLE` outputs without `published_at`
  - rejecting `FAILED` outputs without `publish_error`
  - accepting a valid `PENDING` output

**Step 2: Write failing round-trip tests**

- Add a `prost` encode/decode test that distinguishes:
  - `row_count: None` / `byte_size: None`
  - `row_count: Some(0)` / `byte_size: Some(0)`

**Step 3: Run targeted tests to verify failure**

Run: `cargo test -p arco-proto task_output_contract -- --nocapture`

Expected: compile/test failure because the validation helpers do not exist yet.

### Task 2: Implement minimal contract fixes

**Files:**
- Modify: `crates/arco-proto/src/lib.rs`
- Modify: `proto/arco/v1/orchestration.proto`

**Step 1: Add minimal validation helpers**

- Add a small contract-validation error type to `arco-proto`.
- Add `TaskOutput::validate_contract()` that enforces:
  - `materialization_id` is present
  - visibility state is known and not `_UNSPECIFIED`
  - `PENDING` has no published fields
  - `VISIBLE` has `published_at`, files, and no `publish_error`
  - `FAILED` has `publish_error`, no files, and no published stats/timestamp

**Step 2: Tighten proto comments**

- Remove or soften comment text that claims the current run state already reflects output visibility end-to-end.
- Add comment text clarifying that consumers needing consumability should inspect required task outputs separately.

### Task 3: Verify the contract slice

**Files:**
- Modify: `crates/arco-proto/src/lib.rs`
- Modify: `proto/arco/v1/orchestration.proto`

**Step 1: Run crate tests**

Run: `cargo test -p arco-proto -- --nocapture`

Expected: PASS

**Step 2: Run proto lint**

Run: `buf lint`

Expected: PASS

**Step 3: Run breaking check**

Run: `buf breaking proto --against '.git#ref=HEAD,subdir=proto'`

Expected: PASS
