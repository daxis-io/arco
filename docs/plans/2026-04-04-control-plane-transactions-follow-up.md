# Control-Plane Transactions Follow-Up

This note records the two transaction-runtime follow-ups that were called out
after the initial control-plane transaction landing and are now resolved in the
current repo state.

It is linked from `2026-03-30-control-plane-transactions.md` and is intended to
ship alongside that plan update so the reference is not dangling.

## Current state

- Single-domain transaction runtime support exists for catalog and
  orchestration.
- The shipped transport surface now includes both the protobuf-aligned HTTP
  routes and the tonic gRPC service generated from `transactions.proto`.
- The gRPC transaction surface now reuses the same request-scope validation,
  per-tenant rate limiting, request-id propagation, and request metrics path as
  the HTTP transaction routes.
- `OrchestrationTxReceipt.commit_id` now identifies an immutable orchestration
  commit receipt artifact and is distinct from `revision_ulid`.

## Resolution 1: tonic/gRPC transaction transport

### Current behavior

`transactions.proto` again defines `ControlPlaneTransactionService`, `arco-proto`
generates the tonic client/server modules, and `arco-api` serves the gRPC
transport on `grpc_port` alongside the existing HTTP routes with the same
request-id, auth, rate-limit, and metrics behavior used by the HTTP
transaction endpoints.

### Evidence

- `crates/arco-api/src/grpc_transactions_tests.rs` exercises
  `ApplyCatalogDdl`, `GetCatalogTransaction`, `CommitOrchestrationBatch`,
  `GetOrchestrationTransaction`, `CommitRootTransaction`, and
  `GetRootTransaction` through a real tonic server and generated client.
- The same gRPC integration test file verifies production-mode bearer JWT auth
  and per-tenant rate-limit/request-id metadata behavior on the gRPC transport.
- `crates/arco-api/src/server.rs` binds both HTTP and gRPC listeners in the
  runtime server.

## Resolution 2: distinct orchestration commit identity

### Current behavior

Each successful orchestration transaction now mints a distinct `commit_id`,
persists an immutable receipt at `commits/orchestration/{commit_id}.json`, and
keeps `revision_ulid` as the separately visible manifest revision field.

### Evidence

- `crates/arco-api/src/control_plane_transactions.rs` now generates and writes
  a separate orchestration commit receipt artifact before finalizing the visible
  transaction record.
- `crates/arco-api/tests/control_plane_transactions_api.rs` verifies that
  `commit_id != revision_ulid`, that the immutable commit receipt exists, and
  that idempotent replay returns the same `commit_id`.
