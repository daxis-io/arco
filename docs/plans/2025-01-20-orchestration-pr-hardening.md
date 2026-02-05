# Orchestration PR Hardening: Acceptance Criteria

## Scope

Close the P0/P1 gaps identified in PR review to meet ADR-020/021/022/023 and
the Servo event-driven orchestration execution plan.

## Acceptance Criteria

1. **Deterministic compaction persists out-of-order updates**
   - DispatchEnqueued before DispatchRequested still persists `attempt_id` to
     `dispatch_outbox.parquet`.
   - TimerEnqueued before TimerRequested still persists the canonical `fire_at`
     to `timers.parquet`.
   - L0 deltas include any authoritative field changes even when `row_version`
     does not change.

2. **Merge semantics remain deterministic**
   - `row_version` ordering remains primary.
   - When `row_version` ties, merge uses a deterministic tiebreaker that
     preserves authoritative fields (no data loss).

3. **Attempt guard is end-to-end**
   - Dispatch payload includes `attempt_id`.
   - Worker callbacks carry `attempt_id` and handlers reject mismatches.
   - Compactor fold ignores TaskStarted/Heartbeat/Finished if `attempt_id`
     mismatches the current attempt.

4. **Run completion semantics are correct**
   - Any cancelled task yields `RunState::Cancelled` when all tasks terminal.
   - Failed tasks yield `RunState::Failed` if no cancellations.
   - Otherwise, `RunState::Succeeded`.
   - `tasks_completed` includes cancelled tasks.

5. **Task dispatch lifecycle is consistent**
   - READY -> DISPATCHED transition occurs deterministically (DispatchRequested
     or DispatchEnqueued, per chosen rule).
   - Anti-entropy can observe DISPATCHED tasks using Parquet state.

6. **Controllers are idempotent**
   - ReadyDispatch does not emit multiple DispatchRequested events for the same
     run/task/attempt across repeated reconciliations.

7. **API endpoints are real**
   - Orchestration run endpoints use ledger/compactor projections (no mocks).
   - Worker callback routes are wired and enforce validation/authz.

8. **Tests and observability**
   - Regression tests cover out-of-order dispatch/timer updates.
   - Callback mismatch tests cover attempt_id/attempt validation.
   - Tracing/logs include identifiers for run/task/attempt/dispatch/timer.

## Open Decisions (Confirm Before Coding)

- **Row tie-breaker strategy:** If we keep `row_version` unchanged for older
  events that patch missing fields, define a deterministic merge tiebreaker or
  introduce a monotonic "row_update" signal.
- **Attempt ID in worker contract:** ADR-023 currently documents only `attempt`;
  update contract (and worker SDKs) to include `attempt_id`?
- **DISPATCHED transition:** Should tasks become DISPATCHED at
  DispatchRequested or DispatchEnqueued?

