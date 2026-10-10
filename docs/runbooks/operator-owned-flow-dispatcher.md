# Operator-owned Flow dispatcher: single-host example

Arco exposes the Flow dispatcher and anti-entropy sweeper controllers as
[`dispatcher_service::router`](../../crates/arco-flow/src/orchestration/dispatcher_service.rs)
and [`sweeper_service::router`](../../crates/arco-flow/src/orchestration/sweeper_service.rs).
An operator supplies an `HttpTaskEnqueuer`, chooses the listener and scheduler,
and owns worker delivery. No query engine or task execution runtime is embedded.

The packaged `arco_flow_dispatcher` and `arco_flow_sweeper` also support
`ARCO_FLOW_WORKER_TRANSPORT=http`. Set `ARCO_FLOW_HTTP_INGRESS_URL` and
`ARCO_FLOW_HTTP_INGRESS_TOKEN` on both; no GCP project, location, queue, or
credentials are read in this mode. The binaries POST a JSON object containing
`taskId`, `targetUrl`, `body` (the canonical worker envelope JSON), `audience`,
`headers`, and `routingKey`, with the ingress token as a bearer credential.
The ingress must persist the named task and its delivery metadata before
returning `202`, return `409` only for a durable duplicate, and return `429`
when full. Every other response or timeout leaves the Arco outbox retryable.
The operator ingress owns worker delivery and retries. Scheduled timers are
unsupported in HTTP mode and are never acknowledged as enqueued.

The [filesystem example](../../crates/arco-flow/examples/operator_owned_dispatcher.rs)
is a deployable **single-host proof**, not a qualified production queue. It
uses a private local directory for durable task acceptance and completed-task
receipts, and can use any Arco storage adapter configured for Flow state.

## Build and run

```bash
cargo build -p arco-flow --example operator_owned_dispatcher --features http-client --locked

export ARCO_TENANT_ID=acme
export ARCO_WORKSPACE_ID=analytics
export ARCO_STORAGE_BUCKET=s3://your-flow-state-bucket
export ARCO_OPERATOR_QUEUE_DIR=/var/lib/arco-flow-queue
export ARCO_FLOW_DISPATCH_TARGET_URL=https://your-worker.example/dispatch
export ARCO_FLOW_CALLBACK_BASE_URL=https://your-catalog-api.example
export ARCO_FLOW_WORKER_DISPATCH_SECRET='your-worker-secret'
export ARCO_FLOW_TASK_TOKEN_SECRET='your-at-least-32-byte-token-secret'
export ARCO_FLOW_TASK_TOKEN_ISSUER=your-issuer
export ARCO_FLOW_TASK_TOKEN_AUDIENCE=your-audience

target/debug/examples/operator_owned_dispatcher
```

The example listens on `127.0.0.1:8080` by default. Set
`ARCO_OPERATOR_BIND` to another loopback address if needed. Schedule an
authenticated local `POST /run` for dispatch and `POST /sweeper/run` for
anti-entropy repair at the cadence your Flow workload requires;
an authenticated proxy on the same host can expose those routes to an operator
scheduler. `GET /health` and `GET /sweeper/health` report process liveness. Both
controllers share the same workspace state and file queue. The example drains queued
worker requests every second. `ARCO_FLOW_COMPACTOR_URL` and
`ARCO_FLOW_TIMER_TARGET_URL` are optional. A timer uses the same file queue and
honors its delay. The queue rejects named routing and priority rather than
silently dropping those options.

The queue directory must be private (`0700`); task files are `0600` because
they contain worker headers and task tokens. The queue accepts a task only
after the file and directory are synced. Task ID deduplication survives
reopening, including a regenerated task token after a lost response. A worker
success creates and syncs a `done` link before removing the pending link. A
worker error or lost HTTP response leaves the task pending for retry. Workers
must tolerate at-least-once delivery and validate the active attempt.

## Qualification boundary

Run the local proof with:

```bash
cargo test -p arco-flow --bin arco_flow_dispatcher --bin arco_flow_sweeper \
  --test operator_dispatch_contract --example operator_owned_dispatcher \
  --features http-client --locked
```

The example tests send dispatcher and sweeper work through a loopback HTTP
ingress backed by the file queue. They cover durable acceptance, queue
reopening, lost enqueue responses, worker delivery failure, and
completed-receipt recovery. A separate
test process reopens a test-only disk-backed Flow state and the pending file
queue. The operator sweeper then redrives the old dispatch with a new token;
the loopback worker rejects the expired original token and records a successful
callback for the repair. A rejected task stays pending while later ready tasks
are tried. The expired queue file remains pending and needs an operator
retention policy. The tests do **not** prove power-loss recovery, S3/Azure
state-store qualification, hosted queue durability, multiple queue owners, or
throughput. Completed receipts are retained without a purge policy, and the
single drainer processes deliveries serially. Keep production admission closed
until the chosen operator queue, state provider, and retention policy pass
those gates.
