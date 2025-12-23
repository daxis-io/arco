# ADR-024: Schedule and Sensor Automation

## Status
Accepted

## Context
Layer 2 requires automation triggers: schedules (cron) and sensors (event-driven).
Dagster uses a long-running daemon; we need a serverless alternative.

## Decision
**Hybrid approach:**

1. **Schedules**: Cloud Tasks timers fire at cron intervals
   - Timer emits `ScheduleTicked` event
   - Controller evaluates tick and emits `RunRequested` if due
   - Both events emitted atomically in the same ledger segment
   - Tick history persisted in `schedule_ticks.parquet`
   - Cron expressions accept 5 or 6 fields (seconds optional)

2. **Push Sensors**: Pub/Sub subscription triggers evaluation
   - Message arrives -> sensor evaluates -> emits `SensorEvaluated`
   - **Idempotency key**: `sensor_eval:{sensor_id}:msg:{message_id}`
   - Dedupe enforced at **ledger append** using `OrchestrationEvent.idempotency_key`
   - Ledger rejects duplicate idempotency keys (7-day TTL window sufficient for Pub/Sub retention)
   - Delivery is still at-least-once; duplicates are dropped by the ledger
   - Cursorless by default (message is the cursor)
   - Controller emits `SensorEvaluated` + `RunRequested` atomically

3. **Poll Sensors**: Cloud Tasks polling timer
   - Timer fires -> sensor evaluates with cursor -> emits `SensorEvaluated`
   - Cursor persisted in `sensor_state.parquet`
   - Serialized evaluation via state_version CAS (one at a time per sensor)
   - Controller emits `SensorEvaluated` + `RunRequested` atomically

### Architectural Constraint: Pure Projection Fold

**Critical:** Compactor fold logic MUST NOT emit events. All events are emitted
by controllers atomically in the same ledger segment. This ensures:
- Replay determinism (replaying events produces identical state)
- Clear auditability (controller decisions are explicit in ledger)
- No partial fold state causing duplicate derived events

### Tick History Pattern
Every schedule tick creates a `schedule_ticks` row with:
- `tick_id`: `{schedule_id}:{scheduled_for_epoch}`
- `run_key`: `sched:{schedule_id}:{epoch}`
- `run_id`: Correlated from `RunRequested` event during fold (nullable if no run requested)
- `definition_version`: schedule definition row_version used at tick time
- `asset_selection`: snapshot used to build the run
- `partition_selection`: snapshot used to build the run (optional)

### Catch-up Behavior
On scheduler startup after downtime:
1. Query `last_scheduled_for` from `schedule_state.parquet`
2. Enumerate missed ticks up to `max_catchup_ticks` and within `catchup_window_minutes`
3. Controller emits `ScheduleTicked` + `RunRequested` for each tick

### Poll Sensor CAS Semantics
Poll sensors use compare-and-swap to prevent duplicate triggers from concurrent polls:
- Each `SensorEvaluated` includes `expected_state_version`
- Fold checks `sensor_state.state_version == expected_state_version`
- Stale evaluations are logged and skipped (no run_requests processed)

## Consequences
- No long-running daemon to operate
- Latency bounded by Cloud Tasks/Pub/Sub delivery
- Cursor durability via Parquet (not in-memory)
- Tick history enables "what happened" debugging
- CAS semantics prevent duplicate triggers under concurrent polls
- Atomic event emission ensures replay determinism
