# Orchestration Product Lessons From Rivers Implementation Plan

> **For implementers:** Execute this plan task-by-task with verification after each phase.

**Goal:** Adopt the useful Rivers orchestration product lessons into Arco while preserving Arco's file-native, pointer-published control-plane architecture.

**Architecture:** Treat Rivers as product-shaping prior art, not an architectural dependency. Add contracts, docs, and small implementation slices that clarify execution locations, the local development loop, worker lifecycle, and orchestration observability over Arco's existing primitives: append-first orchestration events, `WorkerDispatchEnvelope`, backend-agnostic dispatch queues, and `system.orchestration.*` system tables. Keep workers external, keep authoritative state ledger/projection/pointer backed, and do not introduce an always-on database as orchestration source of truth.

**Tech Stack:** Rust, Axum, Protobuf/prost, Arrow/Parquet, DataFusion, mdBook, `arco-flow`, `arco-api`, `arco-cli`, object-store CAS, `cargo`, `xtask`.

---

## Source Lessons

Rivers is useful to Arco as product prior art. Its clearest strengths are how it makes "where code runs" and "how a developer runs it locally" obvious. Its storage and runtime assumptions should not be copied into Arco.

### Adopt

1. **Execution location as a first-class product concept**
   - Define the unit that owns user code, runtime image/module, environment, queue binding, and callback capability.
   - Make it clear whether a task is bound to a local process, managed worker pool, Kubernetes deployment, or future hosted runner.
   - Keep the concept scoped by tenant/workspace and compatible with metastore boundaries.

2. **One-command local development loop**
   - Provide a documented `arco dev` target or equivalent local recipe that starts the API, scheduler/flow runtime, local dispatch queue, worker shim, and sample run path.
   - Make the happy path observable through the same state transitions and system tables as production.
   - Avoid a demo-only path that bypasses dispatch, callbacks, or run/task state.

3. **Explicit control-plane and user-code split**
   - Arco plans, schedules, dispatches, records events, folds projections, and exposes read-only state.
   - Workers execute user code and return callbacks with task tokens and attempt identity.
   - The worker boundary should stay centered on `WorkerDispatchEnvelope`.

4. **Runtime state richness**
   - Keep retries, retry waits, timeouts, cancellation, backfills, sensor evaluation, schedule ticks, and partition status explicit.
   - Product docs should name the transition, owning component, event, and queryable evidence.

5. **Orchestration as a queryable product surface**
   - Treat `system.orchestration.*` as the customer-facing evidence layer for runs, tasks, dispatch outbox rows, sensors, schedules, backfills, and partitions.
   - Preserve the current rule that system tables are read-only projections, not request-time enforcement inputs.

6. **Claim-to-contract discipline**
   - Every public orchestration claim needs either a runnable test, an OpenAPI/proto contract, or a documented system table evidence path.
   - Do not advertise offsets, partition mappings, retries, callbacks, or local-dev behavior before contract tests prove the behavior.

### Do Not Adopt

1. **Always-on database source of truth**
   - Do not replace Arco's object-store ledger, projections, and pointer-published state with a DB-backed orchestration authority.
   - A local or test backend can exist only as an adapter; it must not become the source-of-truth model.

2. **Kubernetes-only mental model**
   - Kubernetes can be one execution location backend, not the product definition.
   - Local process, managed queue worker, and hosted runner paths should fit the same contract.

3. **Broad privileged execution surfaces**
   - Worker dispatch must stay scoped by tenant/workspace, queue, attempt, task token, expiry, and callback path.
   - Avoid runtime designs that require broad cluster permissions for ordinary task execution.

4. **Unsafe or opaque user payload defaults**
   - Worker payloads should remain explicit JSON/proto-compatible contracts.
   - Avoid serialization defaults that make untrusted code/data boundaries ambiguous.

5. **Docs ahead of behavior**
   - Product docs should be aspirational only inside plan docs. User-facing docs must match implemented behavior and tests.

---

## Arco Anchors

Use the existing Arco surfaces as the adoption points:

- `docs/guide/src/concepts/orchestration.md` for conceptual framing.
- `docs/guide/src/reference/system-catalog.md` for the current `system.orchestration.*` surface.
- `crates/arco-api/src/system_tables.rs` for the system table allowlist and path mapping.
- `crates/arco-api/src/routes/orchestration.rs` for orchestration API routes.
- `crates/arco-flow/src/orchestration/worker_contract.rs` for `WorkerDispatchEnvelope`.
- `crates/arco-flow/src/dispatch/mod.rs` for backend-agnostic task queue dispatch.
- `crates/arco-flow/src/orchestration/controllers/` for scheduler, sensor, backfill, and task lifecycle ownership.
- `crates/arco-flow/src/orchestration/compactor/fold.rs` for event-to-projection folding.
- `proto/arco/orchestration/v1/orchestration.proto` for stable orchestration contracts.
- `crates/arco-cli/src/main.rs` for the current CLI entry point.

---

## Non-Negotiable Invariants

1. Authoritative orchestration state remains file-native and pointer-published.
2. Execution locations describe where user code runs; they do not own catalog, metastore, or workspace authority.
3. Tenant, workspace, and metastore boundaries stay explicit in every new contract.
4. Workers receive dispatch envelopes and callback with scoped credentials; they do not mutate projections directly.
5. `system.orchestration.*` remains a read-only evidence surface over published projections.
6. Public docs and CLI help cannot claim behavior that is not covered by tests or contract fixtures.
7. Any local development shortcut must use the same run/task/dispatch/callback lifecycle as production.

---

## Delivery Strategy

Ship this as a sequence of small contracts. Start with docs and evidence mapping, then add the smallest CLI/runtime support that lets a developer exercise the same lifecycle locally.

### Task 0: Baseline And Scope Check

**Purpose:** Establish the current orchestration surface before adding new product contracts.

**Read:**

- `docs/guide/src/concepts/orchestration.md`
- `docs/guide/src/reference/system-catalog.md`
- `crates/arco-flow/src/orchestration/worker_contract.rs`
- `crates/arco-flow/src/dispatch/mod.rs`
- `proto/arco/orchestration/v1/orchestration.proto`
- `crates/arco-cli/src/main.rs`

**Commands:**

```bash
git status --short
rg "WorkerDispatchEnvelope|dispatch_outbox|partition_status|sensor_state|backfill" crates docs proto
```

**Exit criteria:**

- The implementer can identify the existing source of truth for worker dispatch, orchestration events, projection folding, API routes, and system tables.
- Any unrelated dirty worktree changes are documented and left untouched.

### Task 1: Write The Orchestration Product Contract

**Purpose:** Make the Rivers-derived lessons explicit in user-facing architecture terms without changing runtime behavior yet.

**Files:**

- Add `docs/guide/src/reference/orchestration-product-contract.md`
- Update `docs/guide/src/SUMMARY.md`
- Optionally update `docs/guide/src/concepts/orchestration.md` with a short link to the reference contract.

**Contract content:**

- Define "execution location" as the deployable user-code boundary.
- Define the control-plane responsibilities: planning, scheduling, dispatching, recording, folding, and publishing.
- Define the worker responsibilities: execute, heartbeat if supported, callback, and respect attempt/token expiry.
- Define the lifecycle from run creation to task dispatch to callback to projection publication.
- Define local development parity: local runs must exercise the same lifecycle.
- Define which `system.orchestration.*` tables prove each transition.

**Verification:**

```bash
cd docs/guide && mdbook build
git diff --check
```

**Exit criteria:**

- The docs clearly state what Arco adopts from Rivers and what it rejects.
- The docs do not imply an implemented `arco dev` command until Task 4 lands.

### Task 2: Define The Execution Location Contract

**Purpose:** Turn "where code runs" into an Arco-owned contract that can support local, managed, Kubernetes, and future hosted execution backends.

**Files:**

- Add `docs/adr/adr-040-execution-locations.md` or equivalent ADR number.
- If code is needed in this slice, update `proto/arco/orchestration/v1/orchestration.proto` only after the ADR is accepted.
- If proto changes are made, update generated Rust/proto fixtures according to the repository workflow.

**Contract fields to evaluate:**

- `execution_location_id`
- `tenant_id`
- `workspace_id`
- Optional `metastore_id` or documented metastore resolution rule
- `kind`: `local`, `managed_queue`, `kubernetes`, `hosted`
- `worker_queue`
- `callback_base_url`
- `runtime_ref` or image/module reference
- Capability flags for cancellation, heartbeat, logs, and artifacts

**Design constraints:**

- Execution location is not a catalog object and does not grant metastore authority by itself.
- Queue binding must remain explicit.
- Callback authority must continue through scoped task tokens.
- Unknown future backends must be representable without changing the run/task state machine.

**Verification if proto changes land:**

```bash
cargo xtask proto-breaking-check
cargo test -p arco-proto
cargo test -p arco-flow --features test-utils --test orchestration_protocol_invariants
```

**Exit criteria:**

- The execution-location concept has a stable owner, scope, and lifecycle.
- Existing dispatch behavior remains backward compatible.

### Task 3: Align Worker Dispatch With Execution Locations

**Purpose:** Ensure the dispatch envelope can carry enough execution-location identity without weakening the worker boundary.

**Files:**

- `crates/arco-flow/src/orchestration/worker_contract.rs`
- `crates/arco-flow/src/dispatch/mod.rs`
- Existing or new tests under `crates/arco-flow/tests/`

**Implementation notes:**

- Add `execution_location_id` only if Task 2 establishes it as a committed contract.
- Preserve `tenant_id`, `workspace_id`, `run_id`, `task_key`, `attempt`, `attempt_id`, `dispatch_id`, `worker_queue`, `callback_base_url`, `task_token`, and `token_expires_at`.
- Keep payloads explicit JSON or proto-compatible values.
- Add serde/proto compatibility fixtures for older envelopes if the field is optional.

**Verification:**

```bash
cargo test -p arco-flow --features test-utils --test worker_dispatch_envelope_tests
cargo test -p arco-flow --features test-utils --test orchestration_protocol_invariants
git diff --check
```

**Exit criteria:**

- Dispatch envelopes still form the sole worker execution contract.
- Execution-location identity, if added, is observable and backward compatible.

### Task 4: Add A Local Development Loop

**Purpose:** Give Arco the product clarity Rivers has around local orchestration, while preserving production lifecycle parity.

**Files:**

- `crates/arco-cli/src/main.rs`
- New `crates/arco-cli/src/commands/dev.rs` if the CLI is modularized.
- `docs/guide/src/getting-started/` or `docs/guide/src/reference/orchestration-product-contract.md`
- Any local runtime fixtures needed under existing test fixture directories.

**Minimum viable `arco dev` behavior:**

- Start or connect to a local API/control-plane process.
- Start the local flow runtime or documented equivalent.
- Use a local dispatch backend.
- Register or load one local execution location.
- Run a sample worker shim that receives `WorkerDispatchEnvelope`.
- Create one run and drive at least one task through dispatch and callback.
- Show the run/task evidence through existing API or `system.orchestration.*` projections.

**Important boundary:**

The local loop can be simple, but it must not bypass the run/task/dispatch/callback lifecycle. If the first slice cannot run the full lifecycle, name it `arco dev --dry-run` or keep it docs-only until it can.

**Verification:**

```bash
cargo test -p arco-cli
cargo test -p arco-flow --features test-utils --test orchestration_dispatch_tests
cd docs/guide && mdbook build
```

**Exit criteria:**

- A developer can understand how to run local orchestration without reading internal modules.
- The local loop proves the same state transitions as production.

### Task 5: Publish A System Table Evidence Matrix

**Purpose:** Make orchestration auditable by mapping product transitions to queryable state.

**Files:**

- Add `docs/guide/src/reference/orchestration-system-table-evidence.md`
- Update `docs/guide/src/SUMMARY.md`
- Update `docs/guide/src/reference/system-catalog.md` if table descriptions need sharpening.
- Tests in `crates/arco-api/tests/system_tables_api.rs` or existing orchestration system-table tests.

**Evidence matrix:**

| Product question | Evidence surface |
| --- | --- |
| What runs exist? | `system.orchestration.runs` |
| What tasks belong to a run? | `system.orchestration.tasks` |
| What dispatches are pending or completed? | `system.orchestration.dispatch_outbox` |
| Why did a sensor fire? | `system.orchestration.sensor_state`, `system.orchestration.sensor_evals` |
| What schedules exist and what tick fired? | `system.orchestration.schedule_definitions`, `system.orchestration.schedule_state`, `system.orchestration.schedule_ticks` |
| What backfill is running? | `system.orchestration.backfills`, `system.orchestration.backfill_chunks` |
| What partition status is known? | `system.orchestration.partition_status` |
| Did a run key conflict? | `system.orchestration.run_key_conflicts` |

**Verification:**

```bash
cargo test -p arco-api --all-features --test system_tables_api
cargo test -p arco-api --all-features --test openapi_orchestration_routes
cd docs/guide && mdbook build
```

**Exit criteria:**

- Every table named in docs is allowlisted and routeable.
- Every major state transition has at least one documented evidence surface.

### Task 6: Add Public-Claim Parity Gates

**Purpose:** Prevent the docs/behavior drift that showed up in Rivers' advertised partition-mapping behavior.

**Files:**

- Existing orchestration tests under `crates/arco-flow/tests/`
- `crates/arco-api/tests/openapi_orchestration_routes.rs`
- Any existing parity or contract-checking xtask, or a small new xtask if the repo already has the pattern.

**Claims to gate:**

- Retry and retry-wait behavior.
- Timeout and cancellation behavior.
- Sensor evaluation and firing behavior.
- Backfill chunk lifecycle.
- Partition status publication.
- Dispatch envelope fields and callback validation.
- Local development lifecycle once `arco dev` exists.

**Verification:**

```bash
cargo test -p arco-flow --features test-utils --test orchestration_protocol_invariants
cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m2
cargo test -p arco-flow --features test-utils --test orchestration_sensor_tests
cargo test -p arco-flow --features test-utils --test orchestration_sensor_e2e_tests
cargo test -p arco-api --all-features --test openapi_orchestration_routes
```

**Exit criteria:**

- User-facing docs only claim behavior covered by tests, proto/OpenAPI contracts, or explicit future-plan language.
- Any unsupported behavior is labeled as future work, not shipped functionality.

### Task 7: Harden The Avoid List

**Purpose:** Encode what Arco should intentionally not borrow.

**Files:**

- `docs/guide/src/reference/orchestration-product-contract.md`
- Security or runtime contract docs if the repo has a standard location.
- Tests around callback/task-token validation if coverage is missing.

**Hardening items:**

- State that orchestration authority is not database-backed.
- State that Kubernetes is one backend, not the product model.
- State that worker callbacks require scoped task tokens and attempt identity.
- State that user payload contracts are explicit and do not rely on unsafe opaque serialization.
- State that execution locations do not grant broad catalog/metastore authority.

**Verification:**

```bash
cargo test -p arco-flow --features test-utils --test worker_dispatch_envelope_tests
cargo test -p arco-api --all-features --test openapi_orchestration_routes
cd docs/guide && mdbook build
```

**Exit criteria:**

- The avoid-list is visible in product contracts, not just tribal knowledge.
- Security-sensitive boundaries are tested or explicitly marked as follow-up work.

### Task 8: Final Integration

**Purpose:** Tie the new contract into the broader Arco product surface without overstating implementation status.

**Files:**

- `README.md`, only if it needs one concise link to the orchestration contract.
- `docs/guide/src/SUMMARY.md`
- Any generated OpenAPI/proto docs affected by earlier tasks.

**Verification:**

```bash
cargo fmt --check
cargo test -p arco-flow --features test-utils --test orchestration_protocol_invariants
cargo test -p arco-api --all-features --test system_tables_api
cargo test -p arco-api --all-features --test openapi_orchestration_routes
cargo xtask adr-check
cd docs/guide && mdbook build
git diff --check
```

**Exit criteria:**

- The product contract, CLI surface, API routes, and system-table evidence agree.
- The README, if changed, links to implemented or clearly planned behavior only.
- No unrelated changes are included.

---

## Review Gates

1. **Gate A: Docs and invariants**
   - Complete Tasks 0 and 1.
   - Review for architecture fit before changing proto or runtime code.

2. **Gate B: Execution-location contract**
   - Complete Task 2.
   - Review tenant/workspace/metastore boundaries and backward compatibility.

3. **Gate C: Worker and local dev loop**
   - Complete Tasks 3 and 4.
   - Review that local dev uses the real lifecycle instead of a bypass.

4. **Gate D: Evidence and parity**
   - Complete Tasks 5 through 7.
   - Review all public claims against tests and system-table evidence.

5. **Gate E: Product integration**
   - Complete Task 8.
   - Review final docs, README links, and verification output.

---

## Acceptance Criteria

- Arco has a clear written contract for execution locations, worker lifecycle, local development, and orchestration evidence.
- The plan preserves Arco's file-native ledger/projection/pointer architecture.
- `WorkerDispatchEnvelope` remains the authoritative worker boundary.
- `system.orchestration.*` remains the customer-facing, read-only evidence surface.
- Local development, when implemented, exercises the same lifecycle as production.
- User-facing docs do not claim behavior without tests, proto/OpenAPI contracts, or explicit future-plan language.
- The final implementation includes verification output for docs, flow tests, API system-table tests, and any proto compatibility checks touched by the work.

---

## Verification Matrix

Run the narrow commands for each slice, then the final set before merge:

```bash
cd docs/guide && mdbook build
cargo fmt --check
cargo test -p arco-cli
cargo test -p arco-flow --features test-utils --test worker_dispatch_envelope_tests
cargo test -p arco-flow --features test-utils --test orchestration_protocol_invariants
cargo test -p arco-flow --features test-utils --test orchestration_dispatch_tests
cargo test -p arco-flow --features test-utils --test orchestration_parity_gates_m2
cargo test -p arco-flow --features test-utils --test orchestration_sensor_tests
cargo test -p arco-flow --features test-utils --test orchestration_sensor_e2e_tests
cargo test -p arco-api --all-features --test system_tables_api
cargo test -p arco-api --all-features --test openapi_orchestration_routes
cargo xtask adr-check
cargo xtask proto-breaking-check
git diff --check
```

Only run `cargo xtask proto-breaking-check` when proto contracts changed.
