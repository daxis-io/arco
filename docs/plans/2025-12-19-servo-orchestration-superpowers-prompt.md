# Servo Orchestration Execution — Superpowers Prompt

> **Copy this prompt to begin execution with `/superpowers:execute-plan`**

---

## The Prompt

```
Execute the implementation plan at `docs/plans/2025-12-19-servo-event-driven-orchestration-execution-plan.md`.

## Context

You are implementing a distributed event-driven orchestration system for the Arco serverless lakehouse platform. This is production-critical infrastructure that will coordinate task execution across Cloud Run workers using Cloud Tasks for durable dispatch.

The system follows the Kubernetes controller pattern: stateless reconciliation from Parquet projections, with durable timers replacing tick loops. All state flows through an append-only ledger and is materialized by a compactor into queryable Parquet tables.

## Non-Negotiable Constraints

### Platform Invariants (MUST preserve)

1. **Append-only ledger** - Events use `DoesNotExist` precondition; no overwrites
2. **Compactor is sole Parquet writer** - Controllers emit events, never write Parquet
3. **Compaction is idempotent** - Same events replayed produce identical Parquet
4. **Manifest CAS publish** - All manifest updates use conditional writes with fencing
5. **Controllers read Parquet only** - NEVER read the JSON ledger; use base + L0 deltas
6. **No bucket listing for correctness** - All paths are manifest-driven

If any implementation violates these, STOP and redesign.

### Critical Corrections Already Applied

These corrections are IN the plan — don't revert them:

1. **Cloud Tasks IDs** - Use dual-identifier pattern: internal `dispatch:{...}` + hashed `d_{sha256...}` for API
2. **24-hour deduplication** - Not 9 days (that's legacy App Engine queues only)
3. **attempt_id guard** - Workers MUST echo attempt_id; compactor rejects stale attempts
4. **Watermark freshness for ALL timers** - Check `manifest.last_processed_at` before timer actions
5. **Per-edge dep_satisfaction** - Never decrement counters; use conditional increment on new satisfaction
6. **Derived events are projection-only** - `TaskBecameReady`, `TaskSkipped`, `RunCompleted` never hit ledger

### Schema Decisions Locked

- `tasks.parquet`: includes `attempt_id` (concurrency guard), `deps_satisfied_count` (derived), `asset_key`/`partition_key` (Layer 2 hooks)
- `dispatch_outbox.parquet`: includes both `dispatch_id` (internal) and `cloud_task_id` (API-safe)
- `timers.parquet`: same dual-ID pattern
- `dep_satisfaction.parquet`: includes `resolution` enum (SUCCESS/FAILED/SKIPPED/CANCELLED)

## Quality Standards

### Test-Driven Development — MANDATORY

For EVERY task:
```
1. Write failing test FIRST
2. Run it — confirm it fails for the RIGHT reason
3. Write MINIMAL code to pass
4. Run it — confirm it passes
5. Refactor if needed (tests still pass)
6. Commit atomically
```

Never write implementation without a failing test. This is not optional.

### Correctness Tests — MUST PASS

Before marking ANY epic complete, these specific tests must exist and pass:

**Compaction:**
- `test_duplicate_event_is_noop` - Same event twice produces identical state
- `test_out_of_order_events_same_result` - Order doesn't affect final state
- `test_cas_conflict_detected_and_retried` - Concurrent writes handled

**Dispatch:**
- `test_duplicate_outbox_no_duplicate_cloud_task` - 409 = success
- `test_cloud_task_id_is_deterministic` - Same input = same hash

**Dependencies:**
- `test_duplicate_task_finished_does_not_double_decrement` - Critical safety
- `test_stale_attempt_event_is_rejected` - attempt_id guard works

**Timers:**
- `test_retry_timer_respects_watermark_freshness` - Compaction lag = reschedule
- `test_heartbeat_check_guards_against_compaction_lag` - No false failures

### Error Handling Standards

Every error MUST have:
- Structured log with context (tenant_id, run_id, task_key, attempt_id)
- Metric emission
- Defined recovery behavior (retry with backoff / skip / fail-fast)
- Explicit test coverage

"Log and continue" is only valid if system state remains consistent.

### Observability Built-In

Every component needs:
- Latency histogram (operation duration)
- Error counter (by error type)
- Queue depth gauge (where applicable)
- Trace context propagation

## Execution Checkpoints

STOP and verify at these points:

- [ ] **After Epic 0**: All ADRs written and internally consistent
- [ ] **After Epic 2**: Compaction correctness tests pass
- [ ] **After Epic 4**: Dispatch E2E loop works — this PROVES THE ENGINE
- [ ] **After Epic 5**: Dependency resolution is duplicate-safe
- [ ] **After Epic 7**: Anti-entropy recovers stuck work

Do not proceed past a checkpoint until verification is complete.

## Anti-Patterns to AVOID

| If you think... | Reality | Do instead |
|-----------------|---------|------------|
| "I'll add tests later" | You won't. Ship bugs. | TDD: test first always |
| "This edge case is rare" | It will happen at 3am | Test the edge case |
| "Counter decrement is safe" | Duplicates over-decrement | Per-edge satisfaction facts |
| "Compaction is fast enough" | It lags. Timers false-fire. | Watermark freshness guard |
| "Log and continue is fine" | State becomes inconsistent | Define recovery behavior |
| "Sequential task IDs work" | Cloud Tasks rejects/delays | Hash-based IDs |

## Reference Materials

- Existing compactor pattern: `crates/arco-catalog/src/compactor.rs`
- Existing dispatcher: `crates/arco-flow/src/dispatch/cloud_tasks.rs`
- CAS publish: `crates/arco-core/src/publish.rs`
- Scoped storage: `crates/arco-core/src/scoped_storage.rs`

## Commit Format

```
<type>(<scope>): <description>

[body: explain WHY, not WHAT]
```

Types: feat, fix, test, refactor, docs
Scopes: orchestration, compactor, dispatcher, timers, schemas

## Success Criteria

Implementation is complete when:
1. All 7 epics implemented with passing tests
2. All correctness tests from the plan pass
3. All 6 platform invariants verified preserved
4. Dispatch E2E loop works (Epic 4)
5. Anti-entropy recovers stuck work (Epic 7)
6. Metrics exported for all components

## Begin

Start with Epic 0: Architecture Decision Records. Write ADR-020 (Orchestration Domain) first.

Track progress with TodoWrite. Commit after each task.
```

---

## Quick Start Commands

### Option 1: Use the execute-plan slash command
```
/superpowers:execute-plan
```
Then paste the prompt above when asked.

### Option 2: Use subagent-driven development in current session
```
Use superpowers:subagent-driven-development

Plan: docs/plans/2025-12-19-servo-event-driven-orchestration-execution-plan.md

[paste the prompt above]
```

### Option 3: Start fresh session with context
```
cd /Users/ethanurbanski/arco
claude

[paste the prompt above]
```

---

## Key Things to Keep in Mind

### 1. This Is Distributed Systems

Distributed systems fail in ways that seem impossible:
- Events arrive out of order
- Events arrive twice (or more)
- Clocks drift
- Processes crash mid-operation
- Networks partition

Every state transition must be safe under these conditions.

### 2. The Compaction Lag Problem

The #1 real-world failure in serverless reconciliation:
- Events are in the ledger but not yet in Parquet
- Timers fire based on stale projection state
- Result: false timeouts, premature retries, duplicate dispatches

The watermark freshness guard exists for this reason. Use it everywhere.

### 3. Idempotency Is Not Optional

Every operation must be safe to retry:
- Cloud Tasks may deliver the same task twice
- Workers may send callbacks twice
- Compaction may process the same event file twice

Design for at-least-once delivery with exactly-once effects.

### 4. The attempt_id Guard Is Critical

Without it, this happens:
1. Task attempt 1 starts
2. Attempt 1 times out, attempt 2 starts
3. Late "attempt 1 succeeded" arrives
4. System thinks attempt 2 succeeded (wrong!)
5. Downstream tasks start incorrectly

The `attempt_id` (ULID) prevents this by rejecting stale events.

### 5. Per-Edge Dependency Satisfaction

The classic bug:
```
on TaskFinished:
  downstream.unmet_deps -= 1  // WRONG: duplicates over-decrement
```

The fix:
```
on TaskFinished:
  if edge not already satisfied:
    mark edge satisfied
    downstream.satisfied_count += 1  // Conditional, idempotent
```

### 6. Layer 2 Is Coming

The schema includes `asset_key`, `partition_key`, and reserved tables for:
- `partition_status.parquet`
- `schedules.parquet`
- `sensors.parquet`

These enable Dagster-like asset automation (auto-materialize, backfills, freshness). Don't remove them.

### 7. Existing Patterns to Follow

The codebase already has established patterns:
- `Compactor::compact_domain` — how to write compaction
- `ScopedStorage` — tenant/workspace isolation
- `PublishPermit` — fenced manifest updates
- `CloudTasksDispatcher::enqueue` — idempotent dispatch

Reuse these. Don't reinvent.

### 8. The Invariants Are Law

If you find yourself thinking "maybe we could skip the ledger here" or "we could read the ledger just this once" — STOP.

The invariants exist because violating them causes:
- Data loss
- Duplicate processing
- Inconsistent state
- Production incidents

There are no exceptions.

---

## Verification Commands

```bash
# Compile check
cargo check --workspace
cargo clippy --workspace -- -D warnings

# Run tests
cargo test --workspace
cargo test -p arco-flow
cargo test -p arco-integration-tests

# Format check
cargo fmt --workspace --check
```

Run these frequently. Don't let errors accumulate.
