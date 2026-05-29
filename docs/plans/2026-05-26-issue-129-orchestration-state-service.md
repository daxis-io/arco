# Issue 129 Orchestration State Service Plan

## Goal

Expose reusable upstream run and task state APIs so embedders do not rebuild
Arco orchestration state machines.

## Strategy

Add a flow-owned orchestration state service that reads the pointer-published
projection, maps it into stable run/task read models, and exposes canonical
cancel semantics over the existing event ledger plus compaction path. API routes
become thin adapters over the shared service instead of owning read-model and
mutation behavior.

This does not introduce a parallel mutable store, does not depend on
`legacy-scheduler::Store`, and does not expose raw `FoldState` as the embedding
contract.

## Tasks

1. Add flow-owned read models in `arco-flow`.
   - `RunStateView`, `TaskStateView`, `RunDetail`, `RunListPage`, `TaskDetail`,
     `OrchestrationSnapshot`, and projection mapping helpers.
   - Preserve existing API-visible state behavior for cancellation and required
     output visibility.

2. Add `OrchestrationStateService`.
   - Read snapshots through `MicroCompactor::load_state()`.
   - Implement `get_run`, `list_runs`, `get_task`, and `cancel_run`.
   - Make cancellation lock-protected, projection-revalidating, idempotent for
     repeated cancel requests, and visible before returning success.

3. Cut API orchestration routes over to the service.
   - Keep auth, workspace scoping, OpenAPI DTOs, and HTTP response shapes in
     `arco-api`.
   - Convert flow read models through explicit route-local adapters.

4. Add issue #129 race regressions.
   - Dispatched but not started tasks remain cancelled when cancellation wins
     before `/started`.
   - `/started` conflicts for cancelled or cancel-requested tasks.
   - Stale finish/failure events cannot reopen terminal success.
   - Task lookup is scoped by `(run_id, task_key)`.

5. Tighten task token scope compatibly.
   - Add optional run and attempt claims.
   - Preserve legacy token decoding and legacy minting API.
   - Mint scoped dispatch tokens where run and attempt are available.
   - Reject run or attempt mismatches only when scoped claims are present.

6. Document embedding semantics.
   - `OrchestrationStateService` is the runtime API.
   - The object-store ledger plus pointer-published projections remain the
     authority.
   - `system.orchestration.*` is read-only evidence and can lag runtime state.
   - Cancellation and retry decisions require projection freshness and active
     attempt identity.

## Verification

Required focused gates:

```bash
cargo test -p arco-flow orchestration::state --lib
cargo test -p arco-flow orchestration::callbacks --lib
cargo test -p arco-api routes::orchestration --lib
cargo test -p arco-api routes::tasks --lib
cargo test -p arco-core task_tokens --lib
mdbook build docs/guide
```

Quality gates:

```bash
cargo fmt --check
git diff --check
```
