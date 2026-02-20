# ADR-032: Engine Boundaries and Split-Service Topology

## Status

Accepted

## Context

Arco now ships a unified codebase that contains catalog APIs, orchestration APIs,
compactors, and worker-dispatch services. Without explicit boundaries, engine and
ownership concerns can drift over time:

1. Query engines may accumulate write responsibilities.
2. API services may bypass ledger/compactor ownership and write state directly.
3. Worker dispatch contracts may become provider-specific and tightly coupled.
4. Legacy orchestration paths may remain ambiguous in production.

This ADR hardens runtime boundaries for split-service deployment and clarifies
what this cycle does not include.

## Decision

### 1. Engine responsibilities are hard-boundaried

- **Orchestrator control plane (`arco-api`, `arco-flow`)**:
  event APIs, run/task state transitions, callback validation, and dispatch intent.
  No direct state Parquet writes.
- **DataFusion (`/api/v1/query`, `/api/v1/query-data`)**:
  read-only query execution (`SELECT`/`CTE` only).
- **Compactors (`arco-compactor`, `arco_flow_compactor`)**:
  sole writers for materialized state/snapshot Parquet paths.
- **DuckDB-WASM (browser)**:
  browser-side read path only via signed URLs minted by API.
- **ETL compute runtime (external workers)**:
  executes task payloads and reports lifecycle callbacks to API.

### 2. Split-service topology is the production model

Services are independently deployable and communicate over explicit HTTP contracts:

- `arco-api`
- `arco-compactor`
- `arco_flow_compactor`
- `arco_flow_dispatcher`
- `arco_flow_sweeper`
- external worker runtime(s)

### 3. Dispatch contract is provider-agnostic and canonical

Dispatcher/sweeper emit a canonical `WorkerDispatchEnvelope` to workers.
Cloud Tasks is the first adapter, but the payload contract is transport/provider agnostic.

### 4. ADR-020 is the production orchestration path

The event-driven orchestration domain (ADR-020) is the production default.
Legacy scheduler modules remain available only via explicit feature flag.

### 5. Immediate breaking payload switch

Legacy dispatch payload formats are removed from dispatcher/sweeper worker calls.
Workers must parse `WorkerDispatchEnvelope`.

### 6. Non-goals for this cycle

- No in-process ETL engine inside API or orchestration services.
- No Spark/dbt/Flink adapter implementation.
- No endpoint removals for `/api/v1/query`, `/api/v1/query-data`, `/api/v1/browser/urls`,
  or task callback endpoints.

## Consequences

- Engine ownership is auditable and CI-enforced.
- Production behavior is less ambiguous (ADR-020 path by default).
- Dispatch payload migration requires coordinated worker + dispatcher/sweeper rollout.
- Split-service operations require explicit environment contract management.
