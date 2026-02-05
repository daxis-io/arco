# Layer 2 Execution Prompt

> Use this prompt to execute the Layer 2 Dagster Parity Automation plan with `/superpowers:execute-plan` or in a fresh Claude Code session.

---

## Execution Prompt

```
Execute the Layer 2 Dagster Parity Automation implementation plan.

## Plan Location
- Main plan: docs/plans/2025-12-22-layer2-automation-execution-plan.md
- P0 corrections reference: docs/plans/2025-12-22-layer2-p0-corrections.md

## Quality Bar

This implementation MUST meet production-grade standards:

### Architectural Integrity
1. **Compactor is pure projection** - Controllers emit ALL events atomically. Compactor NEVER emits new events during fold. This is non-negotiable for replay determinism.
2. **Stable opaque IDs** - All entity IDs (schedule_id, sensor_id, backfill_id) are ULIDs, not human-readable names. Names are display fields only.
3. **HMAC-based run IDs** - run_id = HMAC(tenant_secret, tenant:workspace:run_key). Namespaced and enumeration-resistant.
4. **CAS for cursor updates** - Poll sensor evaluations include expected_state_version; stale evals are dropped.
5. **Separation of concerns** - last_materialization_* (data freshness, success only) vs last_attempt_* (execution status, every attempt).
6. **Compact event payloads** - BackfillCreated uses PartitionSelector, not full partition list.

### Code Quality Standards
- **TDD strictly enforced** - Write failing test FIRST, then minimal implementation, then verify pass
- **DRY/YAGNI** - No speculative abstractions; minimal code that solves the problem
- **Property-based tests** - For state machines, idempotency invariants, and CAS semantics
- **Failure-mode tests** - Duplicate delivery, crash recovery, out-of-order events
- **No silent failures** - All errors logged with structured context; metrics instrumented

### Commit Discipline
- Each task = one atomic commit
- Commit message format: `feat(orchestration): <what changed>`
- Run `cargo check && cargo clippy && cargo test -p arco-flow` before each commit
- Do NOT commit code that doesn't compile or has failing tests

## Execution Flow

### Before Each Epic
1. Read the epic goal and file list
2. Identify any dependencies on prior epics
3. Verify prerequisites are in place

### For Each Task
1. **Read the full task** including all steps
2. **Write the failing test first** - copy the test code from the plan
3. **Run the test** - verify it fails for the expected reason
4. **Implement minimal code** - just enough to pass the test
5. **Run all tests** - ensure no regressions
6. **Run clippy** - fix any warnings
7. **Commit** - with the message from the plan
8. **Move to next task**

### After Each Epic
1. Run full test suite: `cargo test -p arco-flow --all-features`
2. Review: Are the invariants from the plan satisfied?
3. Check: Did we introduce any architectural violations?

## Invariants to Verify

These MUST hold after implementation:

| ID | Invariant | Verification |
|----|-----------|--------------|
| L2-INV-1 | Schedule tick idempotency | Same tick_id → no-op |
| L2-INV-2 | Push sensor idempotency | Same message_id → no-op |
| L2-INV-3 | Poll sensor idempotency | Same cursor_before + epoch → no-op |
| L2-INV-4 | Run key idempotency | Same run_key + fingerprint → same run_id |
| L2-INV-5 | Run key conflict detection | Same run_key + different fingerprint → conflict recorded |
| L2-INV-6 | Backfill state version | Wrong version → rejected |
| L2-INV-7 | Chunk planning idempotency | Same chunk_id → no-op |
| L2-INV-11 | Schedule uses snapshot | Tick uses embedded asset_selection, not current definition |
| L2-INV-12 | Sensor eval CAS | Stale expected_state_version → dropped |

## Red Flags to Watch For

Stop and reassess if you see:

1. **Fold logic calling emit_*()** - Compactor should not emit events
2. **String literals as IDs** - Should be ULIDs
3. **run_id_from_run_key without tenant/workspace** - Missing namespace
4. **Cursor updates without CAS check** - Missing expected_state_version
5. **partition_keys array in BackfillCreated** - Should use PartitionSelector
6. **last_materialization overwritten on failure** - Only update on success
7. **Tests without explicit assertions** - Every test needs clear pass/fail criteria

## Checkpoint Reviews

Pause for review after:
- [ ] Epic 0 complete (ADRs written)
- [ ] Epic 2 complete (Schedule controller works end-to-end)
- [ ] Epic 4 complete (Backfill chunking verified)
- [ ] Epic 6 complete (Run key idempotency + conflicts)
- [ ] Epic 7 complete (API routes functional)

## Success Criteria

Implementation is complete when:

1. All 12 invariants pass their tests
2. `cargo test -p arco-flow` passes with no warnings
3. `cargo clippy -p arco-flow` passes with no warnings
4. Each epic has at least one integration test
5. Property tests cover state machines and idempotency
6. No architectural constraint violations

## Context from Codebase

Key files to reference:
- Layer 1 patterns: crates/arco-flow/src/orchestration/controllers/
- Event types: crates/arco-flow/src/orchestration/events/mod.rs
- Fold logic: crates/arco-flow/src/orchestration/compactor/fold.rs
- Existing tests: crates/arco-flow/tests/orchestration_event_tests.rs

Match the existing code style and patterns. When in doubt, look at how Layer 1 controllers work.

Begin with Epic 0, Task 0.1.
```

---

## Usage

### Option 1: Subagent-Driven (Current Session)
```
/superpowers:subagent-driven-development
```
Then paste the prompt above.

### Option 2: Parallel Session
1. Open a new Claude Code session in this workspace
2. Run `/superpowers:execute-plan`
3. When prompted for the plan, reference: `docs/plans/2025-12-22-layer2-automation-execution-plan.md`

### Option 3: Direct Execution
Paste the prompt directly and begin execution.

---

## Quick Reference Card

### Commands to Run Frequently
```bash
# Test single package
cargo test -p arco-flow

# Test with output
cargo test -p arco-flow -- --nocapture

# Check compilation
cargo check -p arco-flow

# Lint
cargo clippy -p arco-flow

# Format
cargo fmt -p arco-flow
```

### Key Patterns from Layer 1

**Controller structure:**
```rust
impl SomeController {
    pub fn reconcile(&self, state: &State, now: DateTime<Utc>) -> Vec<OrchestrationEvent> {
        // Pure function: read state, emit events
    }
}
```

**Event creation:**
```rust
OrchestrationEvent::new(
    tenant_id,
    workspace_id,
    OrchestrationEventData::SomeEvent { ... }
)
```

**Fold handler:**
```rust
fn fold_some_event(&mut self, event: &OrchestrationEvent) {
    // Update projections only, never emit new events
}
```

### Files to Create (Epic Order)

Note: ADR-026 already exists; verify it instead of rewriting.

1. `docs/adr/adr-024-schedule-sensor-automation.md`
2. `docs/adr/adr-025-backfill-controller.md`
3. `docs/adr/adr-026-partition-status-tracking.md`
4. `crates/arco-flow/src/orchestration/events/automation_events.rs`
5. `crates/arco-flow/src/orchestration/events/backfill_events.rs`
6. `crates/arco-flow/src/orchestration/controllers/schedule.rs`
7. `crates/arco-flow/src/orchestration/controllers/sensor.rs`
8. `crates/arco-flow/src/orchestration/controllers/backfill.rs`
9. `crates/arco-flow/src/orchestration/controllers/partition_status.rs`
10. `crates/arco-api/src/routes/orchestration.rs`
