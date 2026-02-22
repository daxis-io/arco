# Q4 DR Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Harden delta commit and orchestration compactor crash recovery so replay/rebuild is deterministic and CI-enforced.

**Architecture:** Use failpoint-backed integration tests to pin crash-window behavior first, then implement minimal coordinator and orchestration changes to satisfy those invariants. Rebuild logic remains manifest/watermark driven by consuming explicit ledger manifests/paths, never relying on bucket-wide listing for correctness.

**Tech Stack:** Rust (`arco-delta`, `arco-flow`), async integration tests with in-memory storage backends, GitHub Actions CI, parity/runbook markdown.

---

### Task 1: Delta Coordinator Crash-Window Tests (RED)

**Files:**
- Modify: `crates/arco-delta/tests/idempotency_replay.rs`

**Step 1: Write failing tests**
- Add crash-window tests for:
  - crash after reservation before delta-log write
  - crash after delta-log write before idempotency marker
  - crash after idempotency marker before finalize
  - repeated replay returns stable `(version, delta_log_path)`
- Extend test backend to inject failures on specific write attempts per path.

**Step 2: Run targeted test to verify failure**
- Run: `cargo test -p arco-delta --all-features --test idempotency_replay -- --nocapture`
- Expected: new crash-after-idempotency-before-finalize test fails due to stale inflight finalization gap.

**Step 3: Commit red tests**
- `git add crates/arco-delta/tests/idempotency_replay.rs`
- `git commit -m "test(delta): add crash-window DR replay tests"`

### Task 2: Delta Coordinator Recovery Hardening (GREEN)

**Files:**
- Modify: `crates/arco-delta/src/coordinator.rs`
- Modify (if schema/helper changes needed): `crates/arco-delta/src/types.rs`

**Step 1: Implement minimal fix**
- Ensure replay path also performs deterministic inflight cleanup/finalization when safe.
- Preserve idempotency conflict checks and version/path stability.

**Step 2: Run targeted + crate tests**
- Run: `cargo test -p arco-delta --all-features --test idempotency_replay -- --nocapture`
- Run: `cargo test -p arco-delta --all-features`
- Expected: all delta tests pass.

**Step 3: Commit green changes**
- `git add crates/arco-delta/src/coordinator.rs crates/arco-delta/src/types.rs crates/arco-delta/tests/idempotency_replay.rs`
- `git commit -m "fix(delta): harden coordinator replay/finalize recovery"`

### Task 3: Orchestration Rebuild Tests (RED)

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/manifest.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/mod.rs`
- Modify/Create tests in compactor modules and/or `crates/arco-flow/tests/`

**Step 1: Write failing rebuild tests**
- Add deterministic rebuild-from-ledger test that reconstructs projection equivalence from explicit ledger inputs and watermark semantics.
- Add stale-watermark (compactor-falls-behind) recovery test proving rebuild catches projection up safely.
- Include assertion that rebuild does not require bucket listing calls for correctness.

**Step 2: Run targeted test to verify failure**
- Run: `cargo test -p arco-flow --features test-utils --test orchestration_correctness_tests -- --nocapture`
- Expected: new rebuild tests fail before implementation.

**Step 3: Commit red tests**
- `git add <new/updated flow test files>`
- `git commit -m "test(flow): add orchestration rebuild DR tests"`

### Task 4: Orchestration Rebuild Implementation (GREEN)

**Files:**
- Modify: `crates/arco-flow/src/orchestration/compactor/manifest.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/fold.rs`
- Modify: `crates/arco-flow/src/orchestration/compactor/mod.rs`
- Modify: `crates/arco-flow/src/bin/arco_flow_compactor.rs`
- Modify supporting compactor service code as needed

**Step 1: Implement minimal rebuild path**
- Add manifest/watermark-driven rebuild request model.
- Ensure deterministic event ordering/filtering from explicit ledger manifests/paths.
- Keep correctness independent of bucket listing.

**Step 2: Run flow tests**
- Run: `cargo test -p arco-flow --features test-utils --tests`
- Expected: rebuild and existing compactor/controller tests pass.

**Step 3: Commit green changes**
- `git add crates/arco-flow/src/orchestration/compactor/mod.rs crates/arco-flow/src/orchestration/compactor/manifest.rs crates/arco-flow/src/orchestration/compactor/fold.rs crates/arco-flow/src/bin/arco_flow_compactor.rs <supporting files>`
- `git commit -m "feat(flow): add deterministic orchestration rebuild path"`

### Task 5: Runbooks, CI Gates, and Parity Evidence

**Files:**
- Modify: `docs/parity/dagster-parity-11-production-hardening.md`
- Add/Modify: `docs/runbooks/*` (new DR runbook)
- Modify: `.github/workflows/ci.yml`

**Step 1: Document operator procedures**
- Add step-by-step DR runbook:
  - commit coordinator recovery
  - orchestration rebuild
  - post-recovery validation checklist
- Include concrete commands + expected outcomes.

**Step 2: Wire CI proof gates**
- Add explicit targeted DR test invocations to CI in addition to existing suite commands.
- Update parity evidence pointers to new tests/runbook.

**Step 3: Run checks + commit**
- Run: `cargo xtask parity-matrix-check`
- Commit docs/CI updates.

### Task 6: Final Verification Gate

**Step 1: Run required verification commands**
- `cargo test -p arco-delta --all-features`
- `cargo test -p arco-flow --features test-utils --tests`
- `cargo xtask parity-matrix-check`
- Any new targeted DR test binaries added.

**Step 2: Capture evidence in final report**
- Include command outputs (pass/fail summary + key counts).
- Enumerate remaining risks/gaps if any.
