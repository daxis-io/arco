# Tenant identity-root synthetic admission

## Scope

Use the current format-9 control/v1 authority kernel over `IdentityStorage` in a
`test-utils`-only `IdentityStore` probe. Its sole write input is
`SyntheticIdentityMutation`; it exposes no generic state transaction, root
storage, or metastore mutation interface. The production
`ControlMvpStateStore::new` workspace guard remains in force.

Passing this probe proves scoped synthetic commit/read only; it does not permit
`ControlMvpStateStore::new` to admit identity roots in production.
The probe accepts at most 15 commits; the next commit fails before it can
publish a workspace maintenance intent.

Follow-up: the [physical lifecycle child](2026-09-25-tenant-identity-root-physical-lifecycle.md)
extends this test-only probe. The boundaries below describe the original
synthetic commit/read milestone.

## Implementation

1. Start from the verified PR #435 head, including `55636570` and `033f2250`,
   then integrate the probe onto current main before qualification.
   Give the existing kernel a private root-aware initializer. Retain legacy
   `ScopedStorage` only for workspace lifecycle operations.
2. Add the default-disabled identity probe. Its typed mutation commits through
   immutable publication and exact HEAD CAS, then returns an authenticated
   state token. Identity checkpoint, persisted-reference, restore, maintenance,
   and GC entry points fail before storage I/O.
3. Verify identity/workspace/metastore and tenant isolation, restart and
   historical reads, rejected scope/witness/legacy tokens, stale CAS, and
   lost-response reconciliation. Compile-fail examples reject metastore grant
   and storage-governance mutations. Run focused core/catalog tests and rustdoc,
   formatting, and workspace all-target/all-feature Clippy at the final head
   with a separate Cargo target.

## Exit boundary

This child supplies no retained-reference protection or GC qualification.
Production admission requires a separate physical lifecycle proof for
checkpoint publication, root-local retained closures, generation-fenced GC,
and exact recovery after uncertain publication or deletion. Principal event
semantics, revocation, purge, migration, provider qualification, and routing
remain separate gates under ADR-044.
