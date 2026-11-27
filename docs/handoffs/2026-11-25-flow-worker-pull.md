# Flow worker-pull local qualification

The pull profile starts from Arco main `3dec5193208433bbd206e2cffacd0b7934be1007` and serves one fresh tenant/workspace. Flow owns admission, cancellation, dispatch identity, attempts, retry timing, and execution authorization. A permit limits current authorizations. Lease expiry cannot stop a process that is already running.

## Host and worker contract

- Configure `WorkerPull` with one workspace-rooted store, a stable claim key, a callback base URL, an operator credential, distinct worker session credentials, and one to eight permits. The selected manifest binds the claim key, URL, and permit count. Use the profile as the exclusive writer for that workspace.
- The operator submits the existing `AcceptedRunPlan` through `WorkerPull::accept_plan`. Flow preserves the original plan, execution payload, run key, and accepted event identities. The selected revision admits the plan before the dispatcher can publish a pending outbox row. The operator uses `WorkerPull::cancel_run` to order cancellation with the same authority.
- The worker posts `{"requestId":"..."}` to `/api/v1/tasks/claim` with its session credential. Generate one stable ID per pull intent. A selected grant binds the ID to that session for the claim lifetime and 24 hours after release. `busy` and `empty` are observations, so the same ID can be retried. A different session cannot replay an accepted ID.
- The grant includes the existing worker envelope, a permit slot, a generation, the original `startBefore`, and the current `leaseExpiresAt`. The generation identifies the claim within the workspace. Use the envelope's task token on the existing `started`, `heartbeat`, and `completed` paths, and use the authenticated session ID as `workerId`. An active replay preserves the envelope and reports the current lease. A completed replay includes the accepted outcome and time.
- Start before the 15-second lease expires. Starting changes the lease to the task's heartbeat timeout. Each accepted heartbeat can extend that lease within the capability's 24-hour lifetime. An unstarted expiry can reissue the same logical attempt at a higher generation. A running expiry records a regular Flow completion fact and uses Flow's existing retry policy. Completion and matched permit release are one selected revision.

## Recovery and limits

The existing fenced orchestration manifest pointer selects every pull transition. The new schema records the exact accepted facts inline with each immutable successor and carries live permits and bounded replay history forward. A snapshot prepared before a failed CAS is unreachable from the selected parent chain. The next pull proposal uses its new lock sequence as the canonical numeric manifest ID, so an abandoned snapshot cannot occupy the next candidate slot. The existing reconciler protects selected lineage and its artifacts. Frozen accepted plans remain in their immutable accepted-plan namespace.

Call operator-authenticated `POST /run` on the pull router for one control invocation. The push service keeps its own `POST /run` contract. The pull response reports `observedRevision`, how many dispatches or releases advanced, whether immediate work remains, and the earliest lease or retry deadline. Schedule recurring invocations outside the HTTP or worker process. Use a fixed cadence that can observe new work before an old deadline hint. Storage, lock, or uncertain publication errors return a retryable response.

The proof admits up to 128 tasks over the workspace lifetime and retains up to 1,024 claim receipts. One advance processes at most eight changes and selects at most one revision. Conflict retries reload the visible head and re-evaluate. Invocations have a five-second timeout. The work scan follows the scoped selected projection. Capacity checks first recover due current permits from the already selected callback facts.

## Reproduce

Run the Rust checks from the isolated checkout.

```sh
cargo shared test --locked --offline -j 1 -p arco-flow --lib
cargo shared build --locked --offline -j 1 -p arco-flow --example operator_owned_worker_pull
```

Copy the example executable from the target directory reported by `cargo shared metadata --locked --offline --format-version 1 --no-deps` into a new evidence directory. Run the supervisor against that copy with a fresh output directory.

```sh
python3 crates/arco-flow/examples/operator_owned_worker_pull/qualify.py \
  /path/to/copied/operator_owned_worker_pull /path/to/new/wake-proof
```

The supervisor invokes the pull router in a fresh process for each request. Each invocation reopens the same local disk fixture. The driver exercises backlog progress across multiple bounded calls, admission with no wake registration, a later deadline hint that would hide newly ready work, and an earlier hint made stale by a committed heartbeat. The file fixture supports sequential process ownership. Provider operation, live rollout, cross-attempt native writes, and native publication receipts remain outside this qualification.
