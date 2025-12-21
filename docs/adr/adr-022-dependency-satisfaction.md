# ADR-022: Per-Edge Dependency Satisfaction

## Status

Accepted

## Context

DAG orchestration requires tracking when upstream tasks complete so downstream
tasks become READY. The naive approach uses counters:

```rust
// UNSAFE: Counter-based approach
struct Task {
    unmet_deps_count: u32,
}

fn on_task_finished(upstream: &str, downstream: &str) {
    tasks[downstream].unmet_deps_count -= 1;
    if tasks[downstream].unmet_deps_count == 0 {
        tasks[downstream].state = READY;
    }
}
```

**This breaks under event duplication.**

If `TaskFinished(A)` is delivered twice (common in at-least-once systems like
Pub/Sub), the counter decrements twice:

```
Initial:  B.unmet_deps_count = 1  (depends on A)
Event 1:  B.unmet_deps_count = 0  -> B becomes READY (correct)
Event 2:  B.unmet_deps_count = -1 -> underflow or premature re-READY (BUG)
```

This is a **correctness bug** that's hard to detect in testing but causes
production issues.

## Decision

**Use per-edge satisfaction facts instead of counters.**

### Schema: dep_satisfaction.parquet

| Column | Type | Description |
|--------|------|-------------|
| tenant_id | STRING | Tenant identifier |
| workspace_id | STRING | Workspace identifier |
| run_id | STRING | Run identifier |
| upstream_task_key | STRING | Upstream task (PK component) |
| downstream_task_key | STRING | Downstream task (PK component) |
| satisfied | BOOL | Whether edge is satisfied |
| resolution | STRING | SUCCESS, FAILED, SKIPPED, CANCELLED |
| satisfied_at | TIMESTAMP | When edge was satisfied |
| satisfying_attempt | INT32 | Which attempt satisfied the edge |
| row_version | STRING | ULID of satisfying event |

**Primary Key:** `(tenant_id, workspace_id, run_id, upstream_task_key, downstream_task_key)`

### Fold Logic

On `TaskFinished(upstream, succeeded)`:

```rust
fn fold_task_finished(&mut self, event: &TaskFinishedPayload) {
    // 1. Update task state
    self.tasks.get_mut(&event.task_key).map(|task| {
        task.state = match event.outcome {
            Succeeded => TaskState::Succeeded,
            Failed => TaskState::Failed,
            _ => task.state,
        };
        task.row_version = event.event_id.clone();
    });

    // 2. Satisfy downstream edges
    if event.outcome == Succeeded {
        for downstream_key in self.get_dependents(&event.task_key) {
            self.satisfy_edge(
                &event.task_key,
                &downstream_key,
                DepResolution::Success,
                &event.event_id,
            );
        }
    }
}

fn satisfy_edge(
    &mut self,
    upstream: &str,
    downstream: &str,
    resolution: DepResolution,
    event_id: &str,
) {
    // Deterministic event-time derived from the event ULID.
    let event_time = ulid_to_timestamp(event_id);
    let edge_key = (upstream, downstream);

    // Check if edge was already satisfied
    let was_satisfied = self.dep_satisfaction
        .get(&edge_key)
        .map(|e| e.satisfied)
        .unwrap_or(false);

    // Upsert edge (idempotent: same result if called twice)
    self.dep_satisfaction.insert(edge_key, DepSatisfactionRow {
        satisfied: true,
        resolution,
        satisfied_at: event_time,
        row_version: event_id.to_string(),
        ..
    });

    // Only increment derived counter if NEWLY satisfied
    if !was_satisfied {
        let downstream_task = self.tasks.get_mut(downstream).unwrap();
        downstream_task.deps_satisfied_count += 1;

        // Check readiness
        if downstream_task.deps_satisfied_count == downstream_task.deps_total
            && downstream_task.state == TaskState::Blocked
        {
            downstream_task.state = TaskState::Ready;
            downstream_task.ready_at = Some(event_time);
            self.emit_dispatch_request(downstream);
        }
    }
}
```

### Duplicate Safety

The key insight is the **conditional increment**:

```
if !was_satisfied {
    deps_satisfied_count += 1;
}
```

If the same event is processed twice:
- First time: `was_satisfied = false` → increment
- Second time: `was_satisfied = true` → no-op

The edge upsert is also idempotent (same row_version means no change).

### Failure Propagation

When upstream fails terminally:

| Upstream Outcome | Edge Resolution | Downstream Effect |
|------------------|-----------------|-------------------|
| Succeeded | SUCCESS | Satisfies dependency |
| Failed (terminal) | FAILED | Downstream → SKIPPED |
| Skipped | SKIPPED | Downstream → SKIPPED (transitive) |
| Cancelled | CANCELLED | Downstream → CANCELLED |

```rust
fn propagate_failure(&mut self, failed_task: &str) {
    let mut to_skip = VecDeque::new();
    to_skip.push_back(failed_task.to_string());

    while let Some(upstream) = to_skip.pop_front() {
        for downstream in self.get_dependents(&upstream) {
            // Mark edge as resolved (not satisfied)
            self.dep_satisfaction.insert(
                (upstream.clone(), downstream.clone()),
                DepSatisfactionRow {
                    satisfied: false,  // Not satisfied, but resolved
                    resolution: DepResolution::Failed,
                    ..
                }
            );

            // Skip downstream if not already terminal
            if !self.tasks[&downstream].state.is_terminal() {
                self.tasks.get_mut(&downstream).unwrap().state = TaskState::Skipped;
                to_skip.push_back(downstream.clone());
            }
        }
    }
}
```

### Scalability Considerations

For large DAGs (10k+ tasks), avoid O(all edges) operations:

1. **Cluster by downstream_task_key**: Parquet files clustered by downstream
   allow efficient pruning when checking readiness.

2. **Derived counter**: `deps_satisfied_count` in `tasks.parquet` avoids
   COUNT queries at read time. Only the compactor updates it.

3. **Batch processing**: Micro-compactor processes impacted downstream tasks
   in each batch, not all edges.

4. **Materialized view**: For very large DAGs, consider pre-computing
   "all edges for run X" during PlanCreated fold.

## Consequences

### Positive

- **Duplicate-safe**: Critical correctness property for at-least-once delivery
- **Auditable**: Edge satisfaction is a first-class fact, not derived state
- **Queryable**: "What edges are satisfied?" is a simple Parquet query
- **Failure-aware**: Resolution field tracks WHY an edge is/isn't satisfied

### Negative

- **Additional table**: `dep_satisfaction.parquet` adds storage overhead
- **O(out_degree) work**: Each task completion writes N edge rows
- **Join required**: Readiness check needs both `tasks` and `dep_satisfaction`

### Trade-off Analysis

The storage overhead is acceptable:
- Typical DAG: 3-5 edges per task
- Edge row: ~200 bytes
- 10k task run: ~10k edges = ~2MB (trivial for Parquet)

The correctness guarantee is worth the overhead. Counter-based approaches
fail silently under duplication, which is unacceptable for production
orchestration.

## Related ADRs

- ADR-020: Orchestration as Unified Domain (schema context)
- ADR-004: Event Envelope (idempotency_key pattern)
