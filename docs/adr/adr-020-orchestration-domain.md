# ADR-020: Orchestration as Unified Domain

## Status

Accepted

## Context

Servo orchestration needs storage for runs, tasks, timers, and dispatch outbox.
The current MVP uses a Postgres-backed scheduler, but this doesn't align with
Arco's serverless, Parquet-native architecture established in ADR-018.

Following the unified platform pattern, orchestration should become another domain
with its own manifest, ledger path, and Parquet projections. This enables:

- Stateless controllers that reconcile from Parquet projections
- Reuse of the compactor infrastructure
- IAM-enforced sole writer (per ADR-018)
- Event-driven architecture aligned with existing patterns

## Decision

**Add `orchestration` domain following existing patterns.**

### Storage Layout

Following ADR-005 storage layout:

```
{bucket}/{tenant}/{workspace}/
├── ledger/orchestration/{ulid}.json    # Append-only events
├── state/orchestration/{table}/        # Compacted Parquet files
└── manifests/orchestration.manifest.json
```

### Tables

| Table | Purpose | Primary Key |
|-------|---------|-------------|
| `runs.parquet` | Run state and counters | `(tenant_id, workspace_id, run_id)` |
| `tasks.parquet` | Task state machine | `(tenant_id, workspace_id, run_id, task_key)` |
| `dep_satisfaction.parquet` | Per-edge dependency facts | `(tenant_id, workspace_id, run_id, upstream_task_key, downstream_task_key)` |
| `timers.parquet` | Active durable timers | `(tenant_id, workspace_id, timer_id)` |
| `dispatch_outbox.parquet` | Pending dispatch intents | `(tenant_id, workspace_id, dispatch_id)` |

### Deterministic Merge

All tables include `row_version` (event_id ULID) for deterministic merge:

```rust
fn merge_rows(existing: Row, incoming: Row) -> Row {
    // ULID comparison: lexicographically later = newer
    if incoming.row_version > existing.row_version {
        incoming
    } else {
        existing
    }
}
```

For `tasks.parquet`, an additional `state_rank` tiebreaker ensures terminal states
(SUCCEEDED, FAILED, SKIPPED) are preferred when row_versions are equal.

### Controller Pattern

Controllers read base snapshot + L0 deltas from manifest (never ledger):

```rust
trait OrchestrationController {
    /// Read state from manifest-referenced Parquet files
    fn read_state(&self, manifest: &OrchestrationManifest) -> Result<State>;

    /// Compute actions based on state
    fn reconcile(&self, state: &State) -> Vec<Action>;

    /// Execute actions (emit events, create Cloud Tasks)
    fn execute(&self, actions: Vec<Action>) -> Result<Vec<Event>>;
}
```

### Event Categories

Controllers emit only Intent and Acknowledgement facts to the ledger:

| Category | Events | Description |
|----------|--------|-------------|
| Intent | `DispatchRequested`, `TimerRequested` | "I want this to happen" |
| Acknowledgement | `DispatchEnqueued`, `TimerEnqueued` | "External system accepted" |
| Worker Facts | `TaskStarted`, `TaskHeartbeat`, `TaskFinished` | "This happened" |

Derived state changes (`TaskBecameReady`, `TaskSkipped`, `RunCompleted`) are
**projection-only** - computed during compaction fold and written to Parquet,
not emitted as ledger events.

## Consequences

### Positive

- Follows established patterns (minimal new concepts)
- Reuses compactor infrastructure
- IAM-enforced sole writer automatically applies
- Stateless controllers enable horizontal scaling
- Event sourcing provides full audit trail

### Negative

- Eventual consistency for task state (controlled by compaction lag)
- More complex than simple Postgres scheduler
- Requires watermark freshness guards for timer actions

### Mitigations

- Watermark freshness guards prevent stale decisions (ADR-022)
- Anti-entropy sweeper recovers stuck work
- Micro-compaction provides near-real-time visibility (~1-5s lag)

## Related ADRs

- ADR-018: Tier-1 Write Path Architecture (sole writer pattern)
- ADR-021: Cloud Tasks Naming Convention (dispatch ID strategy)
- ADR-022: Per-Edge Dependency Satisfaction (duplicate-safe readiness)
