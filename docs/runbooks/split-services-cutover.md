# Split Services Cutover (Worker Protocol Contract)

This runbook covers production cutover for the published worker protocol contract.
New dispatcher/sweeper writes use canonical camelCase `WorkerDispatchEnvelope` JSON with opaque
`taskId`; readers remain compatible with legacy snake_case dispatch payloads during migration.

## Preconditions

- Staging CI is green for:
  - `cargo test -p arco-worker-contract`
  - `cargo test -p arco-proto --test worker_protocol_contract_tests --test worker_protocol_golden_json`
  - `cargo test -p arco-flow --test worker_dispatch_envelope_tests`
  - `cargo test -p arco-api --test task_token_contract_tests --test openapi_orchestration_routes`
  - `cargo xtask engine-boundary-check`
- External worker image accepts canonical camelCase dispatch envelopes and legacy snake_case
  dispatch envelopes.
- API config includes dedicated `task_token` settings.

## Deploy Order (Strict)

1. Deploy `arco-api` with dedicated task-token config enabled.
- Ensure `ARCO_TASK_TOKEN_SECRET`, `ARCO_TASK_TOKEN_ISSUER`, `ARCO_TASK_TOKEN_AUDIENCE`, and `ARCO_TASK_TOKEN_TTL_SECS` are set.
- Verify callback endpoints still return `401` for invalid tokens and `200/409` for valid callback flows.

2. Deploy external worker runtimes with envelope support.
- Workers must parse `WorkerDispatchEnvelope`.
- Workers must send callbacks using envelope-provided `taskToken`, `callbackBaseUrl`, and `taskId`.
- Workers may fall back to `taskKey` for callbacks only when processing legacy envelopes that omit
  `taskId`.

3. Deploy `arco_flow_dispatcher` and `arco_flow_sweeper` emitting canonical envelope.
- Set `ARCO_FLOW_CALLBACK_BASE_URL` and `ARCO_FLOW_TASK_TOKEN_*` env vars.
- Ensure `ARCO_FLOW_TASK_TOKEN_TTL_SECS` is at least `ARCO_FLOW_TASK_TIMEOUT_SECS + 300` seconds.
- Verify `/run` dispatch cycles enqueue tasks successfully.

4. Enforce production gate for legacy scheduler path.
- Production artifacts should not include `legacy-scheduler` unless explicitly intended.

## Verification Checklist

- Trigger a run from API.
- Confirm dispatch body is canonical camelCase envelope shape with both `taskId` and `taskKey`.
- Confirm worker callbacks (`started`, `heartbeat`, `completed`) are accepted.
- Confirm duplicate `taskKey` values across runs still callback through opaque `taskId`.
- Confirm orchestration state progresses (`DISPATCHED -> RUNNING -> terminal`).
- Confirm `/api/v1/workspaces/{workspace}/runs` and task views reflect updated state.

## Rollback

Rollback must treat dispatcher/sweeper and worker images as a compatibility unit.

1. Roll back `arco_flow_dispatcher` and `arco_flow_sweeper` together.
2. Roll back worker image to the matching payload contract.
3. Keep API task-token config intact unless task callback auth itself regressed.

Do not roll back only one side of the dispatch/worker contract. The canonical writer is
backward-readable, but workers that only understand old dispatch payloads cannot safely consume
canonical `taskId` callbacks.
