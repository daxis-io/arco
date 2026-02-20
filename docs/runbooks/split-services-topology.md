# Split Services Topology

This runbook documents the production split-services topology for Arco boundary-hardened
deployments.

## Services

### `arco-api`

Responsibility:
- Public API surface (`/api/v1/*`, `/iceberg/*`, UC facade when enabled)
- Task callback validation (`/api/v1/tasks/{task_id}/started|heartbeat|completed`)
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

HTTP:
- `GET /health`
- `GET /ready`
- `POST /compact`
- `POST /internal/sync-compact`

### `arco_flow_compactor`

Responsibility:
- Orchestration projection compaction ownership

Required env:
- `ARCO_TENANT_ID`
- `ARCO_WORKSPACE_ID`
- `ARCO_STORAGE_BUCKET`

HTTP:
- `GET /health`
- `POST /compact`

### `arco_flow_dispatcher`

Responsibility:
- Reads orchestration dispatch outbox
- Emits canonical `WorkerDispatchEnvelope`
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

Orchestration write path:
1. API appends orchestration event.
2. API calls `arco_flow_compactor` `POST /compact`.
3. Flow compactor is sole orchestration state writer.

Worker dispatch path:
1. Dispatcher/sweeper enqueue only canonical `WorkerDispatchEnvelope`.
2. Worker calls task callbacks on API with envelope-provided per-task token.
3. API validates task token against dedicated `task_token` config, not user JWT config.

## Non-goals (Current Cycle)

- No in-process ETL execution engine in API/orchestration services.
- No Spark/dbt/Flink adapter implementation.
- No server-side DuckDB runtime for browser query path.
