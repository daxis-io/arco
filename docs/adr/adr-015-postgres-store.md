# ADR-015: Postgres Orchestration Store

## Status
Proposed

## Context
The orchestration engine needs a persistent store for:
- Run state (pending, running, completed, failed)
- Task state and history
- Scheduler coordination (leader election, task claims)

For production deployments, this must be:
- Transactional (ACID for state consistency)
- Highly available (managed services like Cloud SQL, RDS)
- Queryable (for debugging and analytics)

## Decision
Implement `PostgresStore` as the production `Store` implementation:

### Schema Design
```sql
CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    state TEXT NOT NULL,  -- 'pending', 'running', 'succeeded', 'failed'
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 0  -- For optimistic locking
);

CREATE TABLE tasks (
    task_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    task_key TEXT NOT NULL,  -- Canonical task key
    state TEXT NOT NULL,
    attempt INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_tasks_run_id ON tasks(run_id);
CREATE INDEX idx_tasks_state ON tasks(state);
CREATE INDEX idx_runs_tenant_state ON runs(tenant_id, state);
```

### Optimistic Locking
All updates use `WHERE version = $current_version` to detect concurrent modifications. Failed CAS operations return `CasResult::Conflict`.

### Connection Pooling
Use `sqlx::PgPool` with configurable pool size. Default: min=2, max=10.

### Feature Flag
Available under the `postgres` feature flag to avoid mandatory `sqlx` dependency.

## Consequences

### Positive
- Battle-tested, widely deployed database
- Excellent managed service options (Cloud SQL, RDS, Aurora)
- Standard SQL for debugging and ad-hoc queries
- Strong consistency guarantees

### Negative
- Additional infrastructure to operate
- Connection pool management complexity
- Schema migrations needed for updates
- PostgreSQL-specific SQL (not portable to other databases)
