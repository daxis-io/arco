# Issue 255 Orchestration Compaction Architecture Plan

**Goal:** Reduce worker callback latency and workspace-wide compaction lock contention without weakening the ledger, pointer-CAS, or callback visibility guarantees fixed in Batch 2.

**Scope:** Issue #255 only. This is a follow-up implementation plan, not the architecture rewrite itself.

## Current Finding

The callback path still uses `CompactingLedgerWriter`, so started, heartbeat, and completed callbacks append orchestration events and synchronously compact them to visible Parquet state. Batch 2 narrowed one correctness hazard by batching `TaskFinished` plus `TaskOutputVisibilityChanged` for completed callbacks, but it did not change the larger shape: callbacks still acquire the workspace compaction lock, fold from the current manifest state, write L0/base artifacts, and publish a manifest pointer.

The scaling risk remains:

- Heartbeats share the same compacting callback path as lifecycle callbacks.
- Each visible callback publish is serialized by the workspace orchestration compaction lock.
- `MicroCompactor` loads base plus L0 state and diffs full state for small event batches.
- Retention currently prunes `sensor_evals` and `idempotency_keys`; runs, tasks, timers, dependency satisfaction, and dispatch outbox rows are not pruned.

## Batch 0-3 Baseline And Guard

The Batch 0-3 closure work keeps #255 scoped to investigation evidence and a deterministic regression guard. It does not implement the broader heartbeat fast path, fold-state cache, or retention cleanup slices below.

Confirmed local evidence:

- `crates/arco-api/src/routes/tasks.rs` builds task callback dependencies with `CompactingLedgerWriter`, so started, heartbeat, and completed callbacks share the append-and-compact writer.
- `crates/arco-api/src/orchestration_compaction.rs` acquires `locks/orchestration.compaction.lock.json` and requires visible compaction for append-and-compact callback writes.
- `crates/arco-flow/src/orchestration/compactor/service.rs` loads current base plus L0 folded state, folds the callback batch, writes only the state delta, and publishes through the manifest pointer.
- Retention in the compactor remains limited to `sensor_evals` and `idempotency_keys`; run, task, timer, dependency, and dispatch outbox retention belong to Slice 5.

Regression guard:

- `cargo test -p arco-flow --lib callback_compaction_delta_rows_stay_bounded_with_existing_history`

The guard seeds 32 historical runs and tasks, then compacts heartbeat and completion callbacks for one task. The first history delta must contain at least 96 folded rows; the heartbeat callback delta must stay within a 3-row budget (`tasks`, `catalog_run_index`, and `idempotency_keys`), and the completion callback delta must stay within a 4-row budget (`runs`, `tasks`, `catalog_run_index`, and `idempotency_keys`). That gives #255 a reproducible local amplification baseline without changing the compaction architecture in this batch.

## Invariants To Preserve

- Ledger events remain the durable source of truth.
- Manifest pointer CAS and fencing remain the only read-visibility publication mechanism.
- Callback acknowledgement semantics stay explicit: if a route promises visible state, it must verify visible publication.
- Completion/output visibility stays caller-atomic: `TaskFinished` and its immediate visibility event are emitted in one batch.
- Controller reads must never observe state older than the manifest revision they selected.
- Retention must not delete rows needed for retry, zombie recovery, cancellation, audit, or idempotency replay.

## Implementation Slices

### Slice 1: Measurement Gate

Add deterministic latency and amplification instrumentation before changing architecture.

- Record callback compaction duration by route and event type.
- Record lock wait duration, fold load duration, fold apply duration, diff/write duration, and pointer publish duration.
- Record base row counts, L0 count, delta row counts, and bytes read/written per compaction.
- Add a local regression benchmark or ignored load test that seeds increasing run/task history and asserts callback compaction cost does not grow past a documented threshold after later slices.

Exit: #255 has a reproducible baseline for heartbeat and completion callback cost.

### Slice 2: Callback Batching Policy

Keep the Batch 2 `write_events` interface and make batching an explicit callback contract.

- Completed callbacks with output visibility continue to use one batch.
- Evaluate started + first heartbeat coalescing only if it does not delay cancellation visibility.
- Do not batch unrelated runs unless the batch has a stable per-workspace ordering contract and visible-publication receipt.

Exit: callback code paths use batch append/compact when multiple events are causally part of one callback.

### Slice 3: Heartbeat Fast Path

Move high-frequency heartbeat freshness off full synchronous compaction.

Candidate design:

- Keep `TaskHeartbeat` ledger events for audit and rebuild.
- Write a small fenced heartbeat sidecar keyed by `(run_id, task_key, attempt_id)` for live worker liveness.
- Let zombie recovery read the sidecar first, then fall back to folded task state.
- Compact heartbeat ledger events asynchronously or in larger batches for historical views.

Constraints:

- Sidecar writes must be tenant/workspace scoped.
- Sidecar state must not advance stale attempts.
- Rebuild from ledger must still reconstruct durable task history.

Exit: heartbeat callback latency no longer scales with full folded orchestration state.

### Slice 4: Version-Pinned FoldState Cache

Add an in-process cache keyed by visible manifest revision and storage scope.

- Cache `FoldState` loaded from base plus L0.
- Revalidate with pointer object version or manifest revision before applying a batch.
- Invalidate on CAS loss or stale pointer version.
- Bound memory by workspace count and row count.

Exit: repeated callbacks in one process avoid re-reading and rebuilding unchanged base plus L0 state.

### Slice 5: Retention And Pruning

Add conservative terminal-state retention after recovery correctness is covered.

Initial candidates:

- Completed runs and tasks older than a retention window.
- Satisfied dependency rows whose downstream tasks are terminal.
- Dispatch outbox rows once dispatch has terminal acknowledgement or the related task is terminal.
- Expired timers for terminal runs.

Do not prune:

- Active, retry-wait, running, cancelling, or ambiguous-repair rows.
- Rows needed by idempotency replay windows.
- Rows with unresolved output visibility for required-output tasks.
- Rows needed by backfill chunk repair.

Exit: base snapshot size has a bounded retention policy with tests proving live controllers and rebuild still agree.

### Slice 6: ADR-041 L0 Inbox Integration

Route larger event bundles through the ADR-041 L0 inbox work once Batch 3 ordering/rebuild issues are addressed.

- Define bundle ordering by event ULID or explicit sequence, not wall-clock timestamp.
- Keep unknown-event handling as a deployment-skew prerequisite.
- Make remote compactor and inline compactor consume the same bundle contract.

Exit: compaction work is amortized across event bundles without changing reader-visible semantics.

## Tests

- Unit: completed callback emits `TaskFinished` plus visibility in one batch.
- Unit: heartbeat fast path rejects stale attempt sidecar updates.
- Unit: retention never prunes active/retry/running/cancelling rows.
- Property: fold result is identical for the same ordered events split into different batch sizes.
- Integration: increasing historical run/task counts do not linearly increase heartbeat callback latency after the fast path.
- Rebuild: ledger replay after retention produces the same public state for retained windows.

## Non-Goals

- Do not change task token attempt binding here. That belongs to #273.
- Do not change canonical rebuild ordering here. That belongs to #264.
- Do not change idempotency key recording here. That belongs to #263.
- Do not replace offset pagination here. That belongs to #268.
- Do not implement multi-instance chaos coverage here. That belongs to #275.

## Proposed Order

1. Land measurement and the callback batching contract.
2. Add heartbeat sidecar behind a feature flag or config gate.
3. Add the manifest-revision cache for inline compaction.
4. Add retention pruning with controller and rebuild tests.
5. Integrate ADR-041 L0 inbox bundles after Batch 3 ordering issues are fixed.
