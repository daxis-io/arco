# ADR-025: Backfill Controller

## Status
Accepted

## Context
Backfills materialize partition ranges. Need: chunking, concurrency control,
pause/resume/cancel, retry-failed.

## Decision
**Run-per-chunk model:**

### Chunk Planning
1. User requests backfill: `BackfillCreated` event with `PartitionSelector`
2. Controller divides partitions into chunks of `chunk_size`
3. Each chunk -> `BackfillChunkPlanned` event with:
   - `chunk_id`: `{backfill_id}:{chunk_index}`
   - `run_key`: `backfill:{backfill_id}:chunk:{chunk_index}`
   - `partition_keys`: List of partitions in this chunk
4. Controller also emits `RunRequested` atomically with each `BackfillChunkPlanned`

### Compact Event Payloads (P0-6)
`BackfillCreated` uses `PartitionSelector` instead of full partition list:
```rust
pub enum PartitionSelector {
    Range { start: String, end: String },
    Explicit { partition_keys: Vec<String> },
    Filter { filters: HashMap<String, String> },
}
```

Partition resolution happens at chunk planning time, keeping `BackfillCreated`
events compact regardless of backfill size.

### Concurrency Control
- `max_concurrent_runs`: Maximum parallel chunk runs
- Controller counts active chunks, emits next when below limit
- Active = state is RUNNING and run not terminal

### State Machine
```
PENDING -> RUNNING -> SUCCEEDED
              |
           PAUSED -> RUNNING
              |
           FAILED
```

### Pause/Resume
- **Pause**: Stop emitting new `BackfillChunkPlanned`, let active chunks finish
- **Resume**: Continue from next unplanned chunk

### Cancel
- **Cancel**: Stop emitting new chunks, let active runs complete or timeout
- `CANCELLED` is a terminal state reachable from **PENDING**, **RUNNING**, or **PAUSED**
- Transition to `CANCELLED` once no active runs remain (regardless of prior failures)

### Retry-Failed
- Create NEW backfill with `parent_backfill_id`
- Only include failed partitions from parent
- `retry_request_id` for idempotent retry creation

### State Version for Idempotent Transitions
Each state transition increments `state_version`:
```rust
BackfillStateChanged {
    backfill_id: String,
    from_state: BackfillState,
    to_state: BackfillState,
    state_version: u32, // Monotonic
}
// idempotency_key = backfill_state:{backfill_id}:{state_version}:{to_state}
```

### Chunk Idempotency
`BackfillChunkPlanned` idempotency: `backfill_chunk:{backfill_id}:{chunk_index}`

If same chunk_index is planned twice, it's a no-op. This handles:
- Controller restart mid-batch
- Concurrent controller instances

## Consequences
- Granular progress tracking (per-chunk)
- Bounded blast radius (chunk fails, not whole backfill)
- Clean pause/resume semantics
- Retry-failed creates new backfill (no in-place mutation)
- Compact event payloads support million-partition backfills
- State version ensures idempotent transitions
