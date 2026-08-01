# Split Services Topology

This runbook documents the production split-services topology for Arco boundary-hardened
deployments.

## Services

### `arco-api`

Responsibility:
- Public API surface (`/api/v1/*`, `/iceberg/*`, UC facade when enabled)
- Task callback validation (`/api/v1/tasks/{task_id}/started|heartbeat|completed`)
- Worker protocol OpenAPI schemas from `arco-worker-contract`
- Sync compaction RPC clients (catalog and orchestration)

Required env:
- `ARCO_ENVIRONMENT`
- `ARCO_API_PUBLIC`
- `ARCO_DEBUG`
- `ARCO_STORAGE_BUCKET`
- `ARCO_COMPACTOR_URL`
- `ARCO_ORCH_COMPACTOR_URL`
- User JWT auth config: `ARCO_JWT_*`
- Task callback token config: `ARCO_TASK_TOKEN_SECRET`, `ARCO_TASK_TOKEN_ISSUER`, `ARCO_TASK_TOKEN_AUDIENCE`, `ARCO_TASK_TOKEN_TTL_SECS`
- Optional `ARCO_ORCH_TERMINAL_RUN_RETENTION_DAYS` (default `90`; `0` disables): retention window for terminal orchestration runs in the folded projection. Applies to any process that performs orchestration compaction; set it consistently here and on `arco_flow_compactor`.

Health:
- `GET /health`
- `GET /ready`

### `arco-compactor`

Responsibility:
- Tier-1 catalog domain compaction ownership
- Sync compact endpoint for API write path

Required env (service mode):
- `ARCO_TENANT_ID`
- `ARCO_WORKSPACE_ID`
- `ARCO_STORAGE_BUCKET`
- `ARCO_COMPACTOR_PORT` (default `8081`)
- Optional internal OIDC for `/internal/reconcile`: `ARCO_INTERNAL_AUTH_ISSUER`, `ARCO_INTERNAL_AUTH_AUDIENCE`, `ARCO_INTERNAL_AUTH_ALLOWED_SUBS` and/or `ARCO_INTERNAL_AUTH_ALLOWED_EMAILS`, `ARCO_INTERNAL_AUTH_ENFORCE`

HTTP:
- `GET /health`
- `GET /ready`
- `POST /compact`
- `POST /internal/sync-compact`
- `POST /internal/reconcile`

### `arco_flow_compactor`

Responsibility:
- Orchestration projection compaction ownership

Required env:
- `ARCO_TENANT_ID`
- `ARCO_WORKSPACE_ID`
- `ARCO_STORAGE_BUCKET`
- Optional `ARCO_ORCH_DELTA_TOMBSTONES` (default off): enables L0 delta tombstone emission and, with it, orchestration retention. See "Delta tombstone rollout" below — this flag must not be turned on until every process that compacts the workspace understands tombstones, and turning it on does not repair pre-existing history.
- Optional `ARCO_ORCH_TERMINAL_RUN_RETENTION_DAYS` (default `90`; `0` disables): terminal runs (succeeded/failed/cancelled) older than this many days are expired from the orchestration projection via delta tombstones during compaction. Set it consistently on every process that performs orchestration compaction (this service and `arco-api`). Retention only runs when `ARCO_ORCH_DELTA_TOMBSTONES` is enabled, because an expiry that cannot be recorded as a tombstone would resurrect from the delta chain on the next load.
- Optional internal OIDC for `/compact`, `/rebuild`, and `/internal/reconcile`: `ARCO_INTERNAL_AUTH_ISSUER`, `ARCO_INTERNAL_AUTH_AUDIENCE`, `ARCO_INTERNAL_AUTH_ALLOWED_SUBS` and/or `ARCO_INTERNAL_AUTH_ALLOWED_EMAILS`, `ARCO_INTERNAL_AUTH_ENFORCE`

HTTP:
- `GET /health`
- `POST /compact`
- `POST /rebuild`
- `POST /internal/reconcile`

### `arco_flow_dispatcher`

Responsibility:
- Reads orchestration dispatch outbox
- Emits canonical camelCase `WorkerDispatchEnvelope` with opaque `taskId` and semantic `taskKey`
- Enqueues work through provider adapter (Cloud Tasks first)

Required env:
- `ARCO_TENANT_ID`
- `ARCO_WORKSPACE_ID`
- `ARCO_STORAGE_BUCKET`
- `ARCO_FLOW_DISPATCH_TARGET_URL` (worker dispatch endpoint)
- `ARCO_FLOW_CALLBACK_BASE_URL` (API base URL workers call back into)
- `ARCO_FLOW_TASK_TOKEN_SECRET`
- `ARCO_FLOW_TASK_TOKEN_ISSUER`
- `ARCO_FLOW_TASK_TOKEN_AUDIENCE`
- `ARCO_FLOW_TASK_TOKEN_TTL_SECS`
- `ARCO_FLOW_TASK_TIMEOUT_SECS` (default `1800`; token TTL must be >= timeout + 300s callback grace)
- `ARCO_GCP_PROJECT_ID`
- `ARCO_GCP_LOCATION`
- `ARCO_FLOW_QUEUE` (default `arco-flow-dispatch`)

HTTP:
- `GET /health`
- `POST /run`

### `arco_flow_sweeper`

Responsibility:
- Anti-entropy redispatch for stuck orchestration tasks
- Uses same worker envelope contract as dispatcher

Required env:
- `ARCO_TENANT_ID`
- `ARCO_WORKSPACE_ID`
- `ARCO_STORAGE_BUCKET`
- `ARCO_FLOW_DISPATCH_TARGET_URL`
- `ARCO_FLOW_CALLBACK_BASE_URL`
- `ARCO_FLOW_TASK_TOKEN_SECRET`
- `ARCO_FLOW_TASK_TOKEN_ISSUER`
- `ARCO_FLOW_TASK_TOKEN_AUDIENCE`
- `ARCO_FLOW_TASK_TOKEN_TTL_SECS`
- `ARCO_FLOW_TASK_TIMEOUT_SECS` (default `1800`; token TTL must be >= timeout + 300s callback grace)
- `ARCO_GCP_PROJECT_ID`
- `ARCO_GCP_LOCATION`
- `ARCO_FLOW_QUEUE` (default `arco-flow-dispatch`)

HTTP:
- `GET /health`
- `POST /run`

## Cross-Service Contracts

Catalog write path:
1. API appends ledger event.
2. API calls `arco-compactor` `POST /internal/sync-compact`.
3. Compactor is sole snapshot writer.
4. Operators call `POST /internal/reconcile` for current-head repair, and must attach internal OIDC bearer tokens when that gate is enabled.

Orchestration write path:
1. API appends orchestration event.
2. API calls `arco_flow_compactor` `POST /compact`.
3. Flow compactor is sole orchestration state writer.
4. Operators call `POST /internal/reconcile` for current-head legacy-manifest repair, and must attach internal OIDC bearer tokens when that gate is enabled.

Worker dispatch path:
1. Dispatcher/sweeper enqueue canonical `WorkerDispatchEnvelope` from `arco-worker-contract`.
2. Worker executes `taskKey` and calls API callbacks using envelope-provided `taskId`,
   `taskToken`, and `callbackBaseUrl`.
3. API validates task token against dedicated `task_token` config, not user JWT config.
4. API resolves opaque `taskId` back to `(run_id, task_key)` for orchestration state updates.

## Delta Tombstone Rollout (Two Phase)

The orchestration fold removes rows (consumed dispatch outbox entries, expired
terminal runs, expired run-key index entries). Those removals are durable only
when the producing L0 delta references a `deletions` artifact — the *delete
channel*. Enabling that channel changes the manifest in a way an older
compactor cannot honour: it parses `L0Delta` without the `deletions` fields,
drops every tombstone, and can republish a manifest — or a merged base snapshot
— in which every removed row has been resurrected. That is duplicate dispatch,
returning expired runs, and returning dedup keys, and it becomes irreversible
once such a writer wins the manifest CAS.

The rollout is therefore two-phase, gated by `ARCO_ORCH_DELTA_TOMBSTONES`.

### Phase 1 — understand only (default; `ARCO_ORCH_DELTA_TOMBSTONES` unset)

Deploy this build to **every** process that compacts the workspace:
`arco_flow_compactor` and `arco-api` (its sync-compaction path).

In this phase a process:

- reads, applies and preserves tombstones written by any other process;
- rejects a manifest that declares the delete channel but has lost its
  artifacts, instead of folding the stripped state forward;
- emits nothing, so no tombstone-bearing manifest exists for an older binary to
  strip;
- does **not** run retention, because an expiry with no tombstone resurrects.

Do not proceed until every compacting process runs a build with phase-1
support. Verify by rollout revision, not by traffic share: a single old
revision that still serves `/compact` is enough to corrupt the workspace in
phase 2.

### Phase 2 — emit (`ARCO_ORCH_DELTA_TOMBSTONES=1` everywhere)

Set the flag on every compacting process at the same time. From then on each
fold writes a `deletions` artifact for every delta (empty ones included),
stamps `schema_version = 2` on the manifest and a delete-channel marker on each
base snapshot, and runs retention.

The flag is effectively one-way per workspace. A process with the flag *off*
that reads a manifest declaring the channel refuses to compact rather than
appending tombstone-free deltas to it.

### Detection of a stripping writer

`schema_version` is a scalar an older compactor round-trips verbatim, while the
`deletions` references and the base-snapshot marker are fields it drops. A
manifest that declares the channel but lacks those artifacts is therefore
self-contradictory, and compaction fails closed with an error naming the strip
and demanding a rebuild. Recover by rebuilding the projections from the ledger
(`POST /rebuild` on `arco_flow_compactor`) into a clean projection state — do
not attempt to fold the stripped manifest forward.

### Pre-feature history requires a rebuild

Deltas written before this feature existed, and deltas written during phase 1,
carry no tombstones. State reconstructed from them can still contain rows a
fold removed in memory (the resurrection reported as issue #345). **Enabling
phase 2 does not repair that history.** Issue #345 is only closed for a
workspace once its projections have been rebuilt from the ledger with emission
enabled, so that every delta in the surviving chain carries its deletions.
Until that rebuild has run, treat the workspace as still exposed.

Worker protocol compatibility:
- Canonical writes use camelCase JSON.
- Runtime readers accept legacy snake_case dispatch envelopes during migration.
- Legacy callback paths using `taskKey` are accepted only when the key is unambiguous.
- The language-neutral schema lives in `arco.orchestration.v1` protobuf worker messages.

## Non-goals (Current Cycle)

- No in-process ETL execution engine in API/orchestration services.
- No Spark/dbt/Flink adapter implementation.
- No server-side DuckDB runtime for browser query path.
