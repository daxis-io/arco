# ADR-036: Engine Boundaries and Deployment Topology

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

This ADR hardens runtime boundaries and clarifies what this cycle does not
include.

## Decision

### 1. Engine responsibilities are hard-boundaried

- **Orchestrator control plane (`arco-api`, `arco-flow`)**:
  event APIs, run/task state transitions, callback validation, and dispatch intent.
  No direct state Parquet writes.
- **Compaction capability (`arco-api` local Tier1, `arco-compactor`, or
  `arco_flow_compactor`)**: publishes materialized state/snapshot Parquet
  through the same fencing and storage preconditions.
- **Client query engines**:
  choose their own runtime for reads through Arco's scoped, signed URLs.
- **ETL compute runtime (external workers)**:
  executes task payloads and reports lifecycle callbacks to API.

### 2. The operator deploys the API

Arco needs an API layer to operate the catalog. The operator chooses where and
how to deploy that API, its compactors, and its dispatch controllers. Process
placement does not change write ownership: compactors alone publish materialized
Parquet state, and callers use the same API and worker contracts.

The current split-service deployment is one documented topology. Its components
communicate over explicit HTTP contracts:

- `arco-api`
- `arco-compactor`
- `arco_flow_compactor`
- `arco_flow_dispatcher`
- `arco_flow_sweeper`
- external worker runtime(s)

`ARCO_COMPACTOR_URL` selects the remote catalog compactor. Without it, the API
uses local Tier1 compaction, including enabled Iceberg CRUD. The split Cloud
Run deployment remains a recipe. The packaged Flow dispatcher and sweeper
default to Cloud Tasks and can use an operator HTTP ingress.

### 3. Dispatch contract is provider-agnostic and canonical

Dispatcher/sweeper emit a canonical `WorkerDispatchEnvelope` to workers.
The packaged binaries default to Cloud Tasks. The actual worker-send path uses
`HttpTaskEnqueuer` and `enqueue_worker_dispatch`. The public
`dispatcher_service::router` and `sweeper_service::router` let an operator
host both controllers with the same durable queue without changing the worker envelope. With
`ARCO_FLOW_WORKER_TRANSPORT=http`, the packaged binaries send to a durable
operator ingress. Scheduled timers are unsupported by that transport.
The Cloud Tasks-compatible task ID and event field names remain for wire
compatibility; they do not require Cloud Tasks as the transport.

### 4. ADR-020 is the production orchestration path

The event-driven orchestration domain (ADR-020) is the production default.
Legacy scheduler modules remain available only via explicit feature flag.

### 5. Immediate breaking payload switch

Legacy dispatch payload formats are removed from dispatcher/sweeper worker calls.
Workers must parse `WorkerDispatchEnvelope`.

### 6. Non-goals for this cycle

- No in-process ETL engine inside API or orchestration services.
- No Spark/dbt/Flink adapter implementation.
- No endpoint removals for `/api/v1/browser/urls` or task callback endpoints.

## Consequences

- Engine ownership is auditable and CI-enforced.
- Arco does not host SQL execution. Clients supply their own query runtime.
- Production behavior is less ambiguous (ADR-020 path by default).
- Dispatch payload migration requires coordinated worker + dispatcher/sweeper rollout.
- Every deployment must preserve the same compaction and callback authority
  boundaries, whether components share a host or run as separate services.
