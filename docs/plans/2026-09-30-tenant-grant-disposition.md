# Prepared tenant grant disposition

**Goal:** Let the test-only native grant probe recover from a prepared grant whose ledger event never arrived, without allowing a late copy of that grant to become active.

**Design:** Write an explicit no-op abort event at the prepared grant's exact native event ID and sequence. The ledger's existing immutable event path decides the race: either the original grant or the abort occupies the slot. Witness the winning event in the metastore authority before clearing the pending intent. If another event advances the ledger or evidence is ambiguous, keep the pending intent and deny compilation. No timeout or production route is added.

Alternatives considered: deleting the pending intent alone leaves a late writer free to append; an independent abort marker needs a new atomic fence in every ledger writer. Reusing the immutable event slot is the smaller durable fence.

## Implementation and proof

1. Add failing tests in `crates/arco-catalog/tests/tenant_catalog_paths.rs` for restart after an absent event, a late original append, original-wins conflict, concurrent aborts, and replay after another admitted grant.
2. Add a test-only abort mutation in `crates/arco-catalog/src/metastore/events.rs` and no-op replay in `crates/arco-catalog/src/metastore/replay.rs`.
3. Add `TenantCatalogProbe::abort_prepared_grant` in `crates/arco-catalog/src/authz/tenant_probe.rs`. Reuse the existing pending intent, prior-witness checks, native ledger append, and metastore witness transaction. Keep incomplete or conflicting evidence pending.
4. Run focused tests, core/catalog tests, rustdoc compile-fail examples, Clippy, formatting, default-feature check, and hygiene in this worktree's dedicated Cargo target. Record exact source and test receipts, including failures.
5. Commit with DCO, push, and open a PR against main. Leave production identity routing, revocation qualification, and principal purge closed.
