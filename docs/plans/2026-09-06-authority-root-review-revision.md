# Authority Root Review Revision

**Goal:** Keep PR #419 a scope groundwork change without enabling identity through workspace-shaped catalog or state protocols.

**Architecture:** `AuthorityScope` identifies a physical root; `ControlPlaneScope` carries request provenance. Legacy `ScopedStorage` retains a real supplied workspace context and supports only workspace and metastore construction. Tenant identity is a root value and prefix only until a separate identity protocol and versioned state-scope migration are implemented.

**Tech stack:** Rust, Cargo, in-memory storage contract tests, rustdoc compile-fail tests, Markdown architecture decisions.

## Implementation

1. Add a failing contract in `crates/arco-catalog/tests/state_store_control_mvp.rs` proving a metastore physical root cannot use a workspace-shaped `StateScope`, including equal textual IDs. Add a compile-fail example on `MetastoreLedger::new` proving identity storage cannot enter the legacy ledger. Run both against the existing implementation before the fix.
2. In `crates/arco-core/src/authority_root.rs`, remove `workspace_dimension`, expose optional root-specific IDs, make `AuthorityRoot` non-exhaustive, rename durable matching, and add equality/provenance invariants. In `scoped_storage.rs`, remove identity construction and retain actual workspace context separately. Update the ledger caller. In `reader.rs`, `writer.rs`, and `tier1_compactor.rs`, preserve both known IDs in convenience constructors and reject a mismatched explicit metastore ID. Require a workspace physical root in `ControlMvpStateStore::new`. Clarify legacy `StateScope` and authority-storage documentation.
3. Preserve and extend the existing regression tests for distinct request workspaces, shared metastore paths, rejected roots, and byte-identical workspace paths. Identity storage enumeration and mutation are unavailable at the construction boundary in this slice.
4. Reconcile ADR-043, ADR-044, and `metastore-scope-architecture.md`: ownership, current workspace alias, identity freshness, lifecycle, bootstrap, credential vending, migration, and implementation limits.
5. Run focused core/catalog suites, state-store contracts, rustdoc tests, all-target/all-feature Clippy, formatting, and diff checks. Inspect the final diff before a local signed-off follow-up commit. Do not push or post a review.

## Follow-up: Versioned AuthorityScope in StateScope and control/v1

This PR does not change persisted token encoding. Before enabling tenant identity or metastore-rooted `control/v1`, a dedicated change must:

- Carry root kind and its IDs in `StateScope`, `StateToken`, transaction and checkpoint envelopes, manifests, projection intents, continuation tokens, retained references, restore/GC comparisons, and catalog bindings.
- Version the serialized authority representation explicitly. Decode existing workspace-shaped records only as workspace roots; never infer root kind by substituting IDs. Specify unsupported-version rejection, migration qualification, and rollback behavior.
- Prove identity/workspace and metastore/workspace roots with equal textual IDs cannot share tokens, continuations, restore references, or caches; preserve old workspace encoding fixtures and reject cross-root reads before I/O.
- Introduce separate `IdentityMutation` and identity-event provenance with optional originating workspace/metastore dimensions, plus an identity store that cannot accept grants or storage-governance mutations.
- Implement and qualify the cross-root authorization and lifecycle contracts in ADR-044 before production routing. Table authority layout remains a separate decision; the root enum permits future families.
