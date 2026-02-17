# Review Feedback Execution Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Execute code-level review feedback by tightening backfill selector semantics/validation and hardening backfill idempotency behavior under concurrent claim races.

**Architecture:** Keep API and controller behavior aligned by canonicalizing accepted selector inputs to a strict bounds-only filter contract, rejecting ambiguous/unsupported selector payloads early. Preserve current event/compaction flow, but make idempotency record write conflicts replay-safe by resolving the existing record and returning stable responses on concurrent key claims.

**Tech Stack:** Rust (`arco-api`, `arco-flow`), async tests (`tokio`), Axum route tests.

---

### Task 1: Add failing selector validation tests

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`
- Modify: `crates/arco-api/tests/orchestration_api_tests.rs`

**Step 1:** Add unit tests for range date format/order validation and bounds-only filter keys.
**Step 2:** Add API-level tests for invalid range and unsupported filter key payloads.
**Step 3:** Run the targeted tests and verify failure before implementation.

### Task 2: Implement strict selector parsing/canonicalization

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`
- Modify: `crates/arco-flow/src/orchestration/controllers/backfill.rs`

**Step 1:** Canonicalize filter selectors to accepted bounds keys and reject unsupported keys/conflicting aliases.
**Step 2:** Enforce range/filter bounds date parsing and `start <= end`.
**Step 3:** Keep controller bounds extraction behavior aligned (no silent extra-key acceptance).

### Task 3: Add failing idempotency race regression test

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`

**Step 1:** Add a regression test that simulates record precondition failure after append path.
**Step 2:** Verify the test fails against current behavior.

### Task 4: Implement idempotency conflict replay-safe handling

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`

**Step 1:** Return `WriteResult` from idempotency record writes.
**Step 2:** On `PreconditionFailed`, load/resolve existing record and return replay response or explicit conflict.
**Step 3:** Re-run targeted backfill tests to verify behavior.

### Task 5: Verification

**Step 1:** Run focused test targets for updated selector behavior and idempotency race handling.
**Step 2:** Run format + lint for touched files where needed.
