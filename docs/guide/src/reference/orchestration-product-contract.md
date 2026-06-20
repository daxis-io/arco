# Orchestration Product Contract

Arco orchestration owns the product surface for runs, tasks, schedules, sensors,
backfills, worker dispatch, and operational evidence. The useful lesson from
Rivers is product clarity: users should know where code runs, how to run the
same lifecycle locally, and where to inspect state. Arco keeps that clarity
without adopting a database-backed orchestration authority or a Kubernetes-only
runtime model.

This page defines the product contract for those boundaries. It does not add a
new CLI command, protobuf field, dispatch envelope field, or runtime backend by
itself.

## Terms

An execution location is the deployable boundary where user code runs. It can
represent a local process, managed queue worker, Kubernetes deployment, or
future hosted runner. It names runtime identity and routing, but it is not a
catalog object and does not grant catalog, metastore, or workspace authority.
ADR-040 records this architecture contract; this guide does not imply a shipped
runtime registry.

The control plane plans runs, evaluates automation, records append-first events,
folds projections, dispatches work, and publishes read models. It owns
idempotency, attempt identity, task-token validation, retries, cancellation,
and projection publication.

A worker executes user code after receiving a `WorkerDispatchEnvelope`. The
worker reports lifecycle facts through callback endpoints using the scoped task
token and active attempt identity. Workers heartbeat only when the selected
runtime supports heartbeat callbacks. Workers do not write projections directly.

The evidence surface is `system.orchestration.*`. It is a read-only SQL view
over the current published projection plus un-compacted L0 orchestration events,
not the enforcement path for authorization, dispatch, or correctness decisions.

Catalog-facing readers that need run-backed asset metadata use the
manifest-published `catalog_run_index` projection. It is a derived read index
over folded run and task state, not a replacement source of truth and not a
worker-written object path.

## Lifecycle

| Product step | Owner | Contract evidence |
| --- | --- | --- |
| Run request is emitted by automation | Schedule, sensor, or backfill controller | `RunRequested` paired with `ScheduleTicked`, `SensorEvaluated`, or `BackfillChunkPlanned`; `system.orchestration.schedule_ticks`, `system.orchestration.sensor_evals`, `system.orchestration.backfill_chunks`, and conflicts in `system.orchestration.run_key_conflicts` |
| Run becomes executable | API or run bridge | `RunTriggered`; `system.orchestration.runs` |
| Plan is created | Control plane | `PlanCreated`; `system.orchestration.tasks` and `system.orchestration.dep_satisfaction` |
| Task becomes dispatchable | Control plane | `DispatchRequested`; `system.orchestration.dispatch_outbox` |
| Queue accepts dispatch | Dispatch adapter | `DispatchEnqueued`; `system.orchestration.dispatch_outbox` |
| Worker reports callback facts | Worker callback API | `TaskStarted`, optional `TaskHeartbeat`, and `TaskFinished`; `system.orchestration.tasks` |
| Retry wait or timeout is scheduled | Timer controller | `TimerRequested`, `TimerEnqueued`, `TimerFired`; `system.orchestration.timers` |
| Schedule tick is evaluated | Schedule controller | `ScheduleTicked`; `system.orchestration.schedule_ticks` and `system.orchestration.schedule_state` |
| Sensor is evaluated | Sensor controller | `SensorEvaluated`; `system.orchestration.sensor_state` and `system.orchestration.sensor_evals` |
| Backfill is created and chunked | Backfill controller | `BackfillCreated`, `BackfillChunkPlanned`, `BackfillStateChanged`; `system.orchestration.backfills` and `system.orchestration.backfill_chunks` |
| Partition status changes | Projection fold | `system.orchestration.partition_status` |
| Catalog reader enumerates run-backed asset metadata | Projection fold | Manifest-published `catalog_run_index_by_org` artifacts; see [Catalog Run Index](./catalog-run-index.md) |
| Run-key conflict is detected | Projection fold | `system.orchestration.run_key_conflicts` |

Every public orchestration claim should map to one of these event contracts,
system-table evidence paths, OpenAPI/proto contracts, or a runnable test.

## Run Code Version

For public API run triggers planned from a deployed manifest,
`RunTriggered.code_version` and `system.orchestration.runs.code_version`
identify the manifest `codeVersionId` used to plan the run. This is the
pipeline definition or user-code revision, not the API service build identifier
from `ARCO_CODE_VERSION`.

Automation-created runs preserve source metadata when schedule, backfill, or
sensor requests carry a manifest code version. Schedule definitions and
backfills created through the API seed that metadata from the latest deployed
manifest; sensor evaluators must provide the code version in the emitted run
request before the resulting run can make the same code-version claim.

## Callback Responses

`TaskCompletedResponse.finalState` acknowledges the worker-reported outcome for
the completed attempt. For retryable failures it remains `FAILED`; the durable
projection in `system.orchestration.tasks` may subsequently move the task to
retry-wait or redispatched state for the next attempt.

## Local Development Parity

Local development must use the same run, task, dispatch, callback, compaction,
and system-table lifecycle as production. A local shortcut may use an in-memory
or local queue backend, but it must still produce the same durable events and
published evidence.

The deterministic local UAT gate is:

```bash
scripts/run_user_acceptance_pipeline_uat.sh --deterministic
```

That command runs the first-class local user-acceptance pipeline suite together
with the focused Batch 0 reconciliation checks. It is CI-safe and does not run
live GCP, durable-storage, or deployed API/worker gates. Local branch-readiness
checks that combine the deterministic gate with shell smoke tests, formatting,
and diff whitespace checks are available through:

```bash
scripts/run_user_acceptance_pipeline_uat.sh --with-hygiene
```

For constrained local machines or parallel worktrees, cargo subcommands can be
serialized and pointed at an isolated target directory:

```bash
scripts/run_user_acceptance_pipeline_uat.sh --with-hygiene --serial-cargo --isolated-target /tmp/arco-uat-target
```

Clippy remains a separate CI hygiene gate. The deterministic UAT command proves
orchestration correctness; it must not absorb repo-wide lint backlog into the
acceptance signal.

Future durable or deployed UAT output must be validated separately before it is
accepted as evidence:

```bash
tools/validate_user_acceptance_evidence.sh --require-kind durable_storage --require-kind deployed_api_worker target/uat-evidence
```

Durable-storage evidence must use a redacted bucket identifier and include
tenant, workspace, run, plan, schedule, backfill, partition-count, chunk-count,
retry-task, and retry-attempt proof fields for the required scenarios. Deployed
API/worker evidence must include tenant/workspace identity, run identity,
successful run and task rows, satisfied dependencies, catalog-run-index rows,
and partition-status rows for the run under test. Evidence artifacts must not
contain secret-like keys such as tokens, passwords, credentials, or
authorization headers.

The current CLI exposes `arco dev --check` as a check-only workflow. It verifies
CLI/configuration wiring and lists the missing runtime pieces, but it does not
start a local run, deliver work to a worker, process callbacks, compact
projections, or prove `system.orchestration.*` evidence. Its machine-readable
report marks `ready: false` until an end-to-end local loop exists. Until then,
docs should describe the required lifecycle as planned behavior or as a
dry-run/check workflow, not as shipped local orchestration.

## Boundaries

Arco intentionally keeps these boundaries:

- Authoritative orchestration state is the object-store ledger plus
  pointer-published projections.
- `WorkerDispatchEnvelope` is the worker execution contract.
- Execution locations describe where user code runs; they do not grant
  metastore or catalog privileges.
- Kubernetes is one possible execution-location backend, not the product model.
- Task callbacks require scoped tokens and active attempt identity.
- Worker payloads are explicit JSON or proto-compatible contracts.
- System tables are read-only evidence over published projections plus L0
  events, so deterministic UAT can query freshly completed local runs before the
  next compaction publishes a new base projection.
- `catalog_run_index` is a pointer-published read index derived from
  orchestration runs and tasks. Catalog readers should consume it instead of
  listing canonical run-object prefixes.

## Avoided Models

Arco explicitly avoids these product and architecture defaults:

- No database-backed orchestration authority in place of the file-native ledger,
  projections, and published pointers.
- No Kubernetes-only model. Kubernetes can be an execution backend, but local
  processes, managed queue workers, and hosted runners must fit the same
  contract.
- No broad worker privileges. Dispatch and callback authority stay scoped to
  tenant, workspace, queue, attempt, task token, token expiry, and callback
  path.
- No opaque unsafe payload defaults. Worker inputs must stay explicit JSON or
  protobuf-compatible contracts.
- No user-facing docs ahead of behavior. Future behavior belongs in plans or is
  labeled as planned until tests, OpenAPI/proto contracts, or system-table
  evidence prove it.

## Verification Expectations

Before a behavior is described as shipped, it should have one of:

- a focused Rust test for the controller, worker envelope, callback, or API
  route;
- an OpenAPI or protobuf contract test;
- a system-table query test proving the evidence surface is registered and
  routeable;
- a guide page that labels the behavior as planned rather than implemented.

Relevant gates include `worker_dispatch_envelope_tests`,
`orchestration_protocol_invariants`, `orchestration_parity_gates_m2`,
`orchestration_sensor_tests`, `orchestration_sensor_e2e_tests`,
`system_tables_api`, and `openapi_orchestration_routes`.
