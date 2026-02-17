# Batch 3 Prod GO Closure Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close remaining local/code-only Gate 2 signals for Batch 3 with typed-path migration, invariant/failure-injection coverage, and refreshed readiness artifacts.

**Architecture:** Centralize non-catalog canonical paths in `arco-core` typed-path APIs, then migrate flow/API/Iceberg callsites to those APIs. Expand deterministic/property coverage around compaction/order/idempotency semantics and add explicit failure-injection tests for CAS race, partial write recovery, and replay after crash. Refresh runbook and audit artifacts to reference concrete Batch 3 evidence.

**Tech Stack:** Rust (`cargo test`, `clippy`, `fmt`), docs markdown/json artifacts, evidence logs under `release_evidence/`.

---

### Task 1: Typed-path API completion for non-catalog callsites

**Files:**
- Modify: `crates/arco-core/src/flow_paths.rs`
- Modify: `crates/arco-core/src/lib.rs`
- Test: `crates/arco-core/tests/flow_paths_contracts.rs`

**Step 1: Write failing tests**
- Add path contract tests for API manifest/index/idempotency paths and Iceberg pointer/idempotency/transaction/receipt paths.

**Step 2: Run test to verify failure**
- Run: `cargo test -p arco-core flow_paths_contracts -- --nocapture`
- Expected: FAIL for missing typed path functions.

**Step 3: Write minimal implementation**
- Extend `flow_paths.rs` with typed builders for API/Iceberg non-catalog paths.
- Export new path types/functions from `lib.rs`.

**Step 4: Run test to verify pass**
- Run: `cargo test -p arco-core flow_paths_contracts -- --nocapture`
- Expected: PASS.

### Task 2: Migrate flow/API/Iceberg callsites off local literals

**Files:**
- Modify: `crates/arco-api/src/paths.rs`
- Modify: `crates/arco-api/src/routes/manifests.rs`
- Modify: `crates/arco-api/src/routes/orchestration.rs`
- Modify: `crates/arco-api/src/compactor_client.rs`
- Modify: `crates/arco-iceberg/src/paths.rs`
- Modify: `crates/arco-iceberg/src/pointer.rs`
- Modify: `crates/arco-flow/src/orchestration/ledger.rs`
- Modify: `crates/arco-flow/src/outbox.rs`

**Step 1: Write failing tests**
- Add/adjust unit tests asserting callsites now use canonical typed paths.

**Step 2: Run targeted tests to verify failure**
- Run targeted crate tests for changed modules.

**Step 3: Write minimal implementation**
- Replace remaining hardcoded literals with typed path calls.
- Keep wire/storage compatibility unchanged.

**Step 4: Run targeted tests to verify pass**
- Run targeted crate tests for changed modules.

### Task 3: Deterministic/property invariants for out-of-order, duplicate, crash recovery

**Files:**
- Modify: `crates/arco-flow/tests/property_tests.rs`
- Modify: `crates/arco-flow/tests/orchestration_correctness_tests.rs`
- Modify/Create: `crates/arco-flow/tests/orchestration_replay_invariants_tests.rs` (if needed)

**Step 1: Write failing tests**
- Add deterministic/property tests for permutation/duplicate delivery and crash-recovery replay equivalence.

**Step 2: Run targeted tests to verify failure**
- Run focused flow tests to confirm red phase.

**Step 3: Write minimal implementation**
- Fix any uncovered invariant gaps with minimal behavior changes.

**Step 4: Run targeted tests to verify pass**
- Re-run focused flow tests.

### Task 4: Explicit failure-injection tests (CAS race, partial writes, compaction replay)

**Files:**
- Modify: `crates/arco-iceberg/src/pointer.rs`
- Modify: `crates/arco-flow/src/outbox.rs`
- Modify: `crates/arco-flow/tests/orchestration_correctness_tests.rs` (or dedicated test file)

**Step 1: Write failing tests**
- CAS race: concurrent CAS attempts with same expected version, single winner.
- Partial writes: injected write failure mid-batch; retry converges without duplication.
- Compaction replay: replay after crash/restart converges to single-run state.

**Step 2: Run targeted tests to verify failure**
- Run module-specific tests; confirm expected failures.

**Step 3: Write minimal implementation**
- Apply smallest behavior adjustments needed to satisfy invariants.

**Step 4: Run targeted tests to verify pass**
- Re-run module-specific tests.

### Task 5: Runbook + readiness artifacts refresh

**Files:**
- Modify: `docs/runbooks/metrics-catalog.md`
- Modify: `docs/audits/2026-02-12-prod-readiness/findings/gate-2-findings.md`
- Modify: `docs/audits/2026-02-12-prod-readiness/signal-ledger.md`
- Modify: `docs/audits/2026-02-12-prod-readiness/summary.md`
- Modify: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json`

**Step 1: Update docs**
- Record finalized invariants/checks and scriptable verification steps.
- Update signal statuses and reasons with concrete evidence paths.

**Step 2: Validate JSON**
- Run JSON parse/validation check for `gate-tracker.json`.

### Task 6: Verification matrix + archived evidence

**Files:**
- Create: `release_evidence/2026-02-12-prod-readiness/phase-3/batch-3-head/command-matrix-status.tsv`
- Create: `release_evidence/2026-02-12-prod-readiness/phase-3/batch-3-head/command-logs/*.log`

**Step 1: Run required matrix commands**
- Execute all required commands exactly once for Batch 3 head.

**Step 2: Archive logs + status**
- Persist per-command logs and status TSV with start/end UTC and exit code.

**Step 3: Final consistency checks**
- Ensure ledger/audit summary aligns with actual matrix results and signal movements.
