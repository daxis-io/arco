# Control-Plane Transactions Follow-Up

This note tracks two transaction-runtime follow-ups that are explicit in the
current repo state but were not called out clearly in the earlier plan and PI
docs.

It is linked from `2026-03-30-control-plane-transactions.md` and is intended to
ship alongside that plan update so the reference is not dangling.

## Current state

- Single-domain transaction runtime support exists for catalog and
  orchestration.
- The currently shipped transport surface is HTTP-aligned axum endpoints using
  the checked-in protobuf request/response envelopes.
- `OrchestrationTxReceipt.commit_id` currently reuses the visible
  `revision_ulid`.

## Follow-up 1: real tonic/gRPC transaction transport

### Current behavior

`transactions.proto` defines `ControlPlaneTransactionService`, but the checked-in
runtime exposes the transaction APIs through HTTP/protobuf-aligned axum routes.
`arco-api` still carries `grpc_port` configuration and crate-level gRPC wording,
but there is no end-to-end tonic wiring for the transaction service in the
current repo state.

### Why this remains open

The single-domain runtime work intentionally chose the smallest sound transport
integration consistent with the active server architecture instead of adding a
new tonic serving path in the same change.

### Exit criteria

1. Generate and wire a tonic service for `arco.v1.ControlPlaneTransactionService`.
2. Register the transaction service on the API server alongside the existing
   transport surface, or explicitly retire the HTTP-aligned transaction routes.
3. Add transport tests proving `ApplyCatalogDdl`,
   `GetCatalogTransaction`, `CommitOrchestrationBatch`, and
   `GetOrchestrationTransaction` work through the chosen gRPC path.
4. Document the supported transaction transport as a stable operator-facing
   contract.

## Follow-up 2: distinct orchestration commit identity

### Current behavior

`OrchestrationTxReceipt.commit_id` is populated from the visible manifest
`revision_ulid`. This is a compatibility mapping, not proof that orchestration
already exposes a distinct audit-chain commit artifact.

### Why this remains open

The current orchestration publish flow surfaces one stable visible revision
identifier. It does not yet expose a separate commit identity that can fill
`commit_id` independently from `revision_ulid`.

### Exit criteria

1. Decide whether orchestration needs a distinct transaction-visible commit
   artifact beyond the manifest revision.
2. If yes, surface that artifact from the orchestration runtime and use it for
   `OrchestrationTxReceipt.commit_id`.
3. Preserve `revision_ulid` as the visible manifest revision field.
4. Update transaction tests and docs so callers can rely on the final receipt
   identity semantics.
