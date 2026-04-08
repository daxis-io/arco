# Control-Plane Transactions Implementation Plan

> Follow-up note for remaining transport and orchestration receipt identity items: [2026-04-04-control-plane-transactions-follow-up.md](2026-04-04-control-plane-transactions-follow-up.md)

**Goal:** Add a transaction-oriented protobuf surface and shared Rust path/type scaffolding for catalog, orchestration, and tx-scoped root read tokens.

**Architecture:** Introduce a dedicated `transactions.proto` that reuses `RequestHeader`, uses transport-neutral request/response envelopes, and models visibility-scoped transaction receipts/status lookups with explicit `repair_pending` state. Catalog and orchestration stay pointer-published. Root transactions use `transactions/root/{tx_id}.json` plus `transactions/root/{tx_id}.manifest.json` as a pinned super-manifest, without adding a global latest-root pointer or changing ordinary catalog/orchestration readers.

**Tech Stack:** Protobuf, `prost`, Rust, `serde`, `cargo test`.

---

## Scope

- Add a new versioned protobuf contract for:
  - catalog DDL transaction commit + status lookup
  - orchestration batch commit + status lookup
  - root transaction commit + status lookup for pinned multi-domain reads
- Add shared Rust path builders for `transactions/idempotency/...`, per-domain transaction records, `transactions/root/{tx_id}.manifest.json`, and optional `commits/root/{commit_id}.json`.
- Add shared Rust serde types for transaction state records, receipts, and tx-scoped root super-manifests.
- Keep `manifests/root.manifest.json` as the existing catalog/bootstrap object; do not treat it as the root transaction visibility gate.
- Preserve transport/runtime evolution room by separating protobuf envelopes from durable receipt/status payloads.
- Verify with targeted `arco-core` and `arco-proto` tests.

## Verification Matrix

- `cargo test -p arco-core --test control_plane_transaction_paths_contracts -- --nocapture`
- `cargo test -p arco-core --test control_plane_transaction_contracts -- --nocapture`
- `cargo test -p arco-proto --test control_plane_transactions -- --nocapture`
- `buf lint proto/`
- `cargo fmt --all --check`

## Acceptance Criteria

- New protobuf definitions expose the transaction request/response envelopes and receipt/status messages from the proposal.
- The protobuf envelopes leave room for transport-only metadata like `repair_pending`.
- `arco-proto` builds with the new schema file included in code generation.
- `arco-core` exposes canonical transaction paths matching the proposed `transactions/...` object keys.
- Root transaction contracts describe a tx-scoped super-manifest and root read token, not a root pointer CAS visibility gate.
- Shared Rust transaction records serialize with the expected JSON field names and enum casing for storage objects, including tx-scoped root super-manifest references.
