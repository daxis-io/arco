# Orchestration Embedding

`OrchestrationStateService` is the embeddable Rust API for reading run and task
state and for requesting run cancellation. It is owned by `arco-flow` so API
servers, CLIs, tests, and host applications use the same read-model mapping and
mutation semantics.

## State Authority

Authoritative orchestration state is the object-store ledger plus
pointer-published projections. Embedders should not rebuild a separate state
machine from raw events, write a mutable database mirror, or depend on the
legacy scheduler store for current run/task state.

Use the service methods for runtime reads:

- `load_snapshot()` reads the current pointer-published orchestration
  projection.
- `get_run(run_id)` returns a stable run detail read model.
- `list_runs(query)` returns a stable paged run list.
- `get_task(run_id, task_key)` looks up tasks by run id and task key together.
- `cancel_run(request)` appends `RunCancelRequested` and requires visible
  compaction before returning success.

The public read models intentionally do not expose raw `FoldState`. They map
internal projection rows into stable run/task states, task counts, retry and
skip attribution, output visibility, and rerun labels.

## Freshness

`system.orchestration.*` tables are read-only evidence over published
projections. They are useful for SQL inspection, audit, and support workflows,
but they are not the low-latency runtime API and may lag raw event ingestion
until compaction publishes the projection.

Runtime control paths should use `OrchestrationStateService` directly and treat
visible projection publication as the consistency boundary for reads after
mutations.

## Cancellation

Cancellation is cooperative and state-machine guarded.

- A task that has been dispatched but has not started can be cancelled before
  the worker reports `/started`.
- Workers must call `/started` before executing user side effects.
- A `/started` callback for a task that is already terminal or has cancellation
  requested returns a conflict and must not be treated as permission to execute.
- Running workers learn cancellation through heartbeat responses and stop
  cooperatively.

`cancel_run` takes the canonical orchestration compaction lock, reloads state
while holding it, rejects terminal runs unless cancellation was already
requested, appends the cancel event, and requires the resulting projection to be
visible before reporting a requested outcome.

## Attempt Identity

Task callbacks are scoped by task key, tenant, workspace, run id, attempt, and
attempt id. Dispatch-scoped task tokens include optional run and attempt claims;
validators reject mismatches when those claims are present while preserving
legacy token compatibility.

Retry, timeout, and repair decisions must validate the current projection and
active attempt identity before appending new lifecycle events. Stale callbacks
from older attempts must not reopen or overwrite terminal task state.

## Boundaries

Embedders should keep these rules:

- Use `(run_id, task_key)` for task identity. Task keys are not globally unique.
- Do not treat system tables as a synchronous authorization, retry, dispatch, or
  cancellation decision path.
- Do not write projections directly. Append events and compact through the
  canonical orchestration compaction path.
- Do not add broad worker privileges through `callback_base_url`, queue names,
  execution locations, or runtime references. Callback authority stays scoped to
  the token and active attempt.
