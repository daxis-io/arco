# Q3 Remaining Orchestration Closeout Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Finish the remaining Q3 work by landing deterministic diagnostics fixes, tightening parity evidence, and producing a clean Q3 verification/sign-off artifact.

**Architecture:** Treat Q3 remaining work as closeout hardening. Keep API behavior deterministic and replay-safe, prove behavior with parity-gated tests, and ensure evidence docs point only to CI-backed proof. No new feature scope beyond Q3 acceptance criteria.

**Tech Stack:** Rust (`axum`, `serde`, `tokio` tests), Arco orchestration fold/projection model, parity docs (`mdbook` markdown), CI (`cargo test`, `cargo xtask parity-matrix-check`).

---

### Task 1: Deterministic skip attribution index in run read surface

**Files:**
- Modify: `crates/arco-api/src/routes/orchestration.rs`
- Test: `crates/arco-api/tests/orchestration_parity_gates_m1.rs`

**Step 1: Write the failing test**

Add a parity test that creates multiple non-success upstream dependency outcomes for one skipped task and asserts the chosen `skipAttribution` is deterministic (ordered by satisfied timestamp, then upstream task key, then row version).

```rust
#[tokio::test]
async fn parity_m1_skip_attribution_is_deterministic_across_edge_ordering() -> Result<()> {
    // Arrange run graph where task C is skipped and has multiple non-success upstream edges.
    // Assert `skipAttribution` picks the same deterministic first cause.
    Ok(())
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-api --all-features --test orchestration_parity_gates_m1 parity_m1_skip_attribution_is_deterministic_across_edge_ordering`
Expected: FAIL (new assertion not yet satisfied).

**Step 3: Write minimal implementation**

Refactor run read path to precompute a deterministic skip-attribution lookup map once, then map each skipped task from that index.

```rust
fn build_skip_attribution_index(
    fold_state: &FoldState,
    run_id: &str,
) -> HashMap<String, TaskSkipAttributionResponse> { /* deterministic first-cause index */ }

fn task_skip_attribution(
    task: &TaskRow,
    skip_attribution_index: &HashMap<String, TaskSkipAttributionResponse>,
) -> Option<TaskSkipAttributionResponse> { /* lookup */ }
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-api --all-features --test orchestration_parity_gates_m1 parity_m1_skip_attribution_is_deterministic_across_edge_ordering`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-api/src/routes/orchestration.rs crates/arco-api/tests/orchestration_parity_gates_m1.rs
git commit -m "fix(orchestration): make skip attribution deterministic in run reads"
```

### Task 2: Rerun run_key conflict diagnostics parity proof

**Files:**
- Modify: `crates/arco-api/tests/orchestration_parity_gates_m1.rs`
- Verify: `crates/arco-api/src/routes/orchestration.rs`

**Step 1: Write the failing test**

Add a parity test that triggers rerun twice with the same `runKey` but different rerun payload, and asserts conflict `details` includes deterministic introspection fields.

```rust
#[tokio::test]
async fn parity_m1_rerun_run_key_conflict_surfaces_diagnostics_payload() -> Result<()> {
    // Assert conflictType, runKey, existingRunId, existingFingerprint, requestedFingerprint.
    Ok(())
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p arco-api --all-features --test orchestration_parity_gates_m1 parity_m1_rerun_run_key_conflict_surfaces_diagnostics_payload`
Expected: FAIL if rerun conflict branch is missing one or more details fields.

**Step 3: Write minimal implementation**

Ensure rerun conflict branch calls the shared run-key conflict error builder with `.with_details(...)` payload.

```rust
return Err(run_key_conflict_error(&existing, requested_fingerprint));
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p arco-api --all-features --test orchestration_parity_gates_m1 parity_m1_rerun_run_key_conflict_surfaces_diagnostics_payload`
Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-api/tests/orchestration_parity_gates_m1.rs crates/arco-api/src/routes/orchestration.rs
git commit -m "test(orchestration): prove rerun run_key conflict diagnostics payload"
```

### Task 3: Align operator runbook and parity evidence references

**Files:**
- Modify: `docs/runbooks/orchestration-lineage-conflict-diagnostics.md`
- Modify: `docs/parity/dagster-parity-08-run-ux-gaps.md`
- Modify: `docs/parity/dagster-parity-matrix.md`

**Step 1: Write the failing evidence check**

Update docs to require the exact API names/values currently emitted, then run parity evidence checker to catch stale references.

Run: `cargo xtask parity-matrix-check`
Expected: FAIL if any Implemented row lacks accurate CI-backed evidence.

**Step 2: Write minimal documentation updates**

- Fix partition status query parameter examples to `assetKey`.
- Use current deterministic rerun reason values (`from_failure_unsucceeded_tasks`, `subset_selection`).
- Add the rerun conflict diagnostics parity test to Q3 evidence sections.

```markdown
- Tests: `crates/arco-api/tests/orchestration_parity_gates_m1.rs` (`parity_m1_rerun_run_key_conflict_surfaces_diagnostics_payload`)
```

**Step 3: Run evidence check to verify it passes**

Run: `cargo xtask parity-matrix-check`
Expected: PASS.

**Step 4: Commit**

```bash
git add docs/runbooks/orchestration-lineage-conflict-diagnostics.md docs/parity/dagster-parity-08-run-ux-gaps.md docs/parity/dagster-parity-matrix.md
git commit -m "docs(parity): refresh Q3 diagnostics and lineage evidence references"
```

### Task 4: Full Q3 verification gate and release evidence snapshot

**Files:**
- Modify: `docs/plans/2026-02-20-q3-orchestration-leadership-layer.md`
- Create: `docs/plans/2026-02-21-q3-verification-evidence.md`

**Step 1: Run full Q3 command suite**

Run:
```bash
cargo test -p arco-api --all-features --test orchestration_parity_gates_m1
cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m1
cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m2
cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m3
cargo test -p arco-api --all-features --test orchestration_schedule_e2e_tests
cargo test -p arco-api --all-features --test orchestration_api_tests
cargo xtask parity-matrix-check
```
Expected: all PASS.

**Step 2: Record concrete evidence artifact**

Create a dated evidence note containing:
- exact command list
- execution timestamp
- branch and commit SHA
- pass/fail result per command
- any known residual risks

```markdown
# Q3 Verification Evidence (2026-02-21)
- Branch: codex/q3-orchestration-leadership-layer
- Commit: <sha>
- Result: all Q3 gates passing
```

**Step 3: Update Q3 plan status from in-progress to closed**

Update `docs/plans/2026-02-20-q3-orchestration-leadership-layer.md` to include:
- final acceptance criteria checklist marked complete
- link to `docs/plans/2026-02-21-q3-verification-evidence.md`
- explicit note: no open Q3 code gaps remain

**Step 4: Commit**

```bash
git add docs/plans/2026-02-20-q3-orchestration-leadership-layer.md docs/plans/2026-02-21-q3-verification-evidence.md
git commit -m "chore(q3): add final verification evidence and close remaining items"
```

### Task 5: Branch hygiene and handoff

**Files:**
- Modify: none (git/log only)

**Step 1: Rebase/sync if needed**

Run: `git fetch origin && git rebase origin/main`
Expected: clean rebase or documented conflict resolution.

**Step 2: Final smoke check after rebase**

Run:
```bash
cargo test -p arco-api --all-features --test orchestration_parity_gates_m1
cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m3
cargo xtask parity-matrix-check
```
Expected: PASS.

**Step 3: Prepare handoff summary**

Include:
- list of commits for Q3 remaining work
- changed files grouped by API/tests/docs
- proof that exit criteria are met
- any deferred items explicitly marked as non-Q3 scope
