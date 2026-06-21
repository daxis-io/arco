# Local Orchestration Development

Arco does not yet ship an end-to-end `arco dev` loop that starts the API,
flow runtime, local dispatch queue, worker shim, callbacks, compaction, and
system-table evidence path.

The shipped CLI surface for this slice is check-only:

```bash
arco dev --check
```

This command verifies CLI/configuration wiring and lists the missing runtime
pieces. It does not start a run, enqueue work, invoke a worker, call back to the
API, compact projections, or prove `system.orchestration.*` evidence. Its JSON
output reports `ready: false` until a production-equivalent local loop exists.

## Current Local Pipeline UAT

Use the no-cloud local pipeline UAT when you need proof that Arco can write
real data, catalog it, query it, commit a Delta log, and move an orchestrated
run through dispatch and callbacks without GCP:

```bash
bash scripts/run_local_pipeline_uat.sh
```

This is an in-process UAT backed by in-memory storage. It is stronger than a
unit-only contract check, but it is still not a shipped `arco dev` daemon loop:
it does not start separate long-running services, Cloud Scheduler, Cloud Tasks,
Cloud Run, or GCS.

To run the same no-cloud UAT inside a Linux container:

```bash
bash scripts/run_local_pipeline_uat_container.sh
```

This requires a running Docker daemon.

## Current Local Checks

Use the repository checks that exercise the existing contracts:

```bash
cargo test -p arco-integration-tests --test local_pipeline_uat -- --nocapture
cargo test -p arco-cli
cargo test -p arco-flow --features test-utils --test orchestration_dispatch_tests
cargo test -p arco-flow --features test-utils --test worker_dispatch_envelope_tests
cargo test -p arco-flow --features test-utils --test orchestration_protocol_invariants
```

These checks cover the local data pipeline UAT, CLI wiring, dispatch event
flow, worker dispatch envelope compatibility, and orchestration protocol
invariants. They are not a substitute for a full local lifecycle.

## Required Before A Real Loop

A future end-to-end local loop must prove all of the following before docs can
describe it as shipped:

- start or connect to a local API/control-plane process;
- run the local flow runtime using the same controllers as production;
- load a real local execution-location planning or registry source;
- use a local dispatch adapter that delivers `WorkerDispatchEnvelope` to a
  worker;
- run a sample worker shim that calls the task callback API with the scoped
  task token and active attempt identity;
- create a sample run through the API;
- compact and publish run, task, dispatch, and callback evidence through the
  existing API or `system.orchestration.*` projections.

Execution-location identity must come from planning or a real registry. It is
not inferred from `worker_queue`, `kind`, `callback_base_url`, or `runtime_ref`.
Until that source exists, local dispatch envelopes omit `execution_location_id`.
