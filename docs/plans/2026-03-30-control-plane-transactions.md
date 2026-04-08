# Control-Plane Transactions Implementation Plan

**Goal:** Add a transaction-oriented protobuf surface and shared Rust path/type scaffolding for catalog, orchestration, and root control-plane commits.

**Architecture:** Introduce a dedicated `transactions.proto` that reuses `RequestHeader`, uses RPC-specific request/response envelopes, and models visibility-scoped transaction receipts/status lookups with explicit `repair_pending` state. Add shared `arco-core` transaction path builders plus serializable record/receipt structs that match the new `transactions/...` storage layout without changing existing runtime writers yet.

**Tech Stack:** Protobuf, `prost`, Rust, `serde`, `cargo test`.

---

## Scope

- Add a new versioned protobuf contract for:
  - catalog DDL transaction commit + status lookup
  - orchestration batch commit + status lookup
  - root transaction commit + status lookup
- Add shared Rust path builders for `transactions/idempotency/...`, per-domain transaction records, and root transaction manifests.
- Add shared Rust serde types for transaction state records, receipts, and root-manifest references.
- Preserve transport/runtime evolution room by separating RPC envelopes from durable receipt/status payloads.
- Verify with targeted `arco-core` and `arco-proto` tests.

## Verification Matrix

- `cargo test -p arco-core --test control_plane_transaction_paths_contracts -- --nocapture`
- `cargo test -p arco-core --test control_plane_transaction_contracts -- --nocapture`
- `cargo test -p arco-proto --test control_plane_transactions -- --nocapture`
- `buf lint proto/`
- `cargo fmt --all --check`

## Acceptance Criteria

- New protobuf definitions expose the transaction RPCs and receipt/status messages from the proposal.
- RPCs use request/response envelopes that leave room for transport-only metadata like `repair_pending`.
- `arco-proto` builds with the new schema file included in code generation.
- `arco-core` exposes canonical transaction paths matching the proposed `transactions/...` object keys.
- Shared Rust transaction records serialize with the expected JSON field names and enum casing for storage objects, including root manifest identity metadata.
