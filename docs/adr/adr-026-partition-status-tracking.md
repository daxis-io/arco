# ADR-026: Partition Status Tracking

## Status
Accepted

## Context
Need to answer: "Which partitions are stale/missing/failed?"
Two approaches: derive from tasks vs explicit materialization events.

## Decision
**Explicit materialization tracking with separation of concerns (P0-5):**

### Critical: Separate Materialization from Attempt

Track two distinct concepts:
1. **Data freshness** (`last_materialization_*`): Only updated on SUCCESS
2. **Execution status** (`last_attempt_*`): Updated on EVERY attempt

This prevents a failed retry from overwriting valid prior materialization data.

### Schema: partition_status.parquet

| Column | Type | Description |
|--------|------|-------------|
| tenant_id | STRING | Tenant identifier |
| workspace_id | STRING | Workspace identifier |
| asset_key | STRING | Asset identifier |
| partition_key | STRING | Partition identifier |
| last_materialization_run_id | STRING | Run that last materialized (success only) |
| last_materialization_at | TIMESTAMP | When last materialized (success only) |
| last_materialization_code_version | STRING | Code version used (success only) |
| last_attempt_run_id | STRING | Most recent attempt (any outcome) |
| last_attempt_at | TIMESTAMP | When last attempted (any outcome) |
| last_attempt_outcome | STRING | SUCCEEDED/FAILED/CANCELLED |
| stale_since | TIMESTAMP | When became stale (nullable; derived at query time or precomputed) |
| stale_reason_code | STRING | FRESHNESS_POLICY/UPSTREAM_CHANGED/CODE_CHANGED (nullable; derived or precomputed) |
| partition_values | MAP<STRING,STRING> | Dimension key-values |
| row_version | STRING | ULID of last update |

### Update Logic

```rust
impl PartitionStatusRow {
    /// Called when task finishes (any outcome)
    pub fn record_attempt(&mut self, run_id: &str, at: DateTime<Utc>, outcome: TaskOutcome) {
        self.last_attempt_run_id = Some(run_id.to_string());
        self.last_attempt_at = Some(at);
        self.last_attempt_outcome = Some(outcome);
    }

    /// Called ONLY when task succeeds with materialization
    pub fn mark_materialized(&mut self, run_id: &str, at: DateTime<Utc>, code_version: &str) {
        self.last_materialization_run_id = Some(run_id.to_string());
        self.last_materialization_at = Some(at);
        self.last_materialization_code_version = Some(code_version.to_string());
        self.stale_since = None; // Clear precomputed staleness on fresh materialization
    }
}
```

### Staleness Computation

**Computed at query time** (not during fold):
1. Check freshness policy deadline against `last_materialization_at`
2. Check if upstream partitions materialized after this one
3. Check if `current_code_version != last_materialization_code_version`

Requires `last_materialization_code_version` from metadata and `current_code_version`
from asset definitions.

Optional: Periodic staleness sweep controller can precompute and persist
`stale_since` and `stale_reason_code` to speed up reads. If not precomputed,
the query layer derives these values before display status is computed.

### Display Status (Computed)

```rust
// Assumes staleness fields are computed in the query layer or precomputed by a sweep.
pub fn compute_display_status(partition: &PartitionStatusRow) -> PartitionDisplayStatus {
    match (&partition.last_materialization_at, &partition.last_attempt_outcome) {
        (None, _) => PartitionDisplayStatus::NeverMaterialized,
        (Some(mat_at), Some(TaskOutcome::Failed))
            if partition.last_attempt_at > Some(*mat_at) => {
            PartitionDisplayStatus::MaterializedButLastAttemptFailed
        }
        (Some(_), _) if partition.stale_since.is_some() => PartitionDisplayStatus::Stale,
        (Some(_), _) => PartitionDisplayStatus::Materialized,
    }
}
```

### Decoupling from Tasks
- Task state: "execution attempt" - transient
- Partition status: "data freshness" - durable answer to "is my data current?"
- A task can fail but partition remains MATERIALIZED from prior successful run

### Event Flow

1. `TaskFinished` with `outcome: Succeeded` and `asset_key + partition_key`
2. Fold calls `partition.record_attempt(...)` always
3. Fold calls `partition.mark_materialized(...)` only on success with materialization

## Consequences
- Clean separation of execution vs data status
- Query-time staleness enables dynamic policies
- Failed retries don't corrupt materialization history
- Additional storage for partition status tracking
- Code version enables staleness detection on deploy
