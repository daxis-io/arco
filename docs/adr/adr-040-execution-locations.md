# ADR-040: Execution Locations

## Status

Accepted

## Context

Arco already has the core orchestration mechanics: append-first events,
projection folding, backend-agnostic dispatch queues, worker callbacks, and the
canonical `WorkerDispatchEnvelope`. The product still needs a stable concept
for "where user code runs" that can cover local development, managed workers,
Kubernetes deployments, and future hosted runners without turning any one
backend into the product model.

The current dispatch surface carries routing data such as `worker_queue`,
`callback_base_url`, task token, and attempt identity. Those fields are enough
for current dispatch, but they do not name the deployable runtime boundary,
metastore resolution rule, or backend capabilities as an Arco-owned contract.

## Decision

Define an execution location as the orchestration-owned deployable boundary for
user code. It is owned by the orchestration/control-plane product surface, not
by the catalog namespace.

An execution location is modeled with these contract fields when it becomes a
persisted/runtime object:

- `execution_location_id`: stable execution-location identity within its scope.
- `tenant_id`: tenant that owns the execution-location configuration.
- `workspace_id`: workspace that can bind runs to the location.
- `metastore_id`, or an explicit workspace-to-metastore resolution rule. If
  `metastore_id` is absent, dispatch planning must resolve metastore context
  from the workspace binding before issuing work; the execution location itself
  never supplies metastore authority.
- `kind`: initially `local`, `managed_queue`, `kubernetes`, or `hosted`. Future
  backend kinds must fit the same lifecycle and capability contract.
- `worker_queue`: the explicit queue or queue family used for dispatch. Queue
  binding is never inferred only from `kind` or `runtime_ref`.
- `callback_base_url`: callback route root for the selected backend. It is
  routing metadata, not authority.
- `runtime_ref`: image, module, local entry point, or hosted runtime reference.
  It identifies code/runtime placement, not credentials.
- capability flags for cancellation, heartbeat, logs, and artifacts. These
  flags are declarative backend capabilities; unsupported capabilities must not
  require new run or task states.
- lifecycle state such as `active`, `draining`, `disabled`, or `archived`.

Execution locations are not catalog objects. They do not grant catalog,
workspace, storage, or metastore authority by themselves. `workspace_id` and
optional `metastore_id` scope the configuration and resolution path; they are
not permissions. Authorization remains owned by the request context,
catalog/metastore contracts, compiled permission state, scoped task tokens, and
active attempt identity.

Worker dispatch remains centered on `WorkerDispatchEnvelope`. The dispatch
envelope continues to carry explicit tenant/workspace/run/task identity,
attempt identity, queue routing, callback routing, token, and token expiry.
Callback authority comes from validating the scoped task token against the
active run, task, attempt, and expiry; `callback_base_url` only tells the worker
where to call back.

Runtime dispatch now carries an optional `execution_location_id` on
`WorkerDispatchEnvelope` as the compatibility slice for execution-location
identity. It remains backward compatible, is omitted when unset, and is covered
by serde compatibility tests. Older envelopes without an execution-location
field continue to execute through the existing queue and callback contract.

Unknown future backends are represented by adding a backend kind and
capabilities, not by changing the run/task state machine. The existing states
for queued, dispatched, running, retry wait, cancellation, timeout, success,
and failure remain the product lifecycle.

Local development uses the same contract. A local execution location may route
to an in-memory or local queue adapter, but it must still exercise the run,
task, dispatch, callback, event, compaction, and evidence lifecycle.

## Non-Goals

- Do not replace object-store ledger and pointer-published projections with a
  database-backed orchestration source of truth.
- Do not make Kubernetes the default product abstraction.
- Do not use execution locations as a shortcut for metastore authorization.
- Do not make `callback_base_url`, `runtime_ref`, or `worker_queue` grant
  callback, workspace, catalog, or metastore authority.
- Do not advertise an `arco dev` lifecycle until it exists and is tested.
- Do not change proto messages until a later compatibility slice explicitly
  accepts that runtime work. `WorkerDispatchEnvelope` changes must remain
  optional and compatibility-safe.

## Consequences

- Product docs can describe local, managed queue, Kubernetes, and hosted worker
  backends with one vocabulary.
- Dispatch can remain backend-agnostic while still gaining a stable execution
  identity in a later compatibility-safe slice.
- User-facing docs must distinguish the accepted execution-location contract
  from currently implemented CLI/runtime behavior.
- Proto and registry work can be deferred: this ADR accepts the product and
  architecture contract plus the optional dispatch identity field, not a shipped
  execution-location registry.
- Future API, proto, or system-table additions for execution locations need
  contract tests and published projection evidence before being marked shipped.

## Related ADRs

- ADR-020: Orchestration as Unified Domain
- ADR-023: Worker Contract Specification
- ADR-035: System Catalog Tables
