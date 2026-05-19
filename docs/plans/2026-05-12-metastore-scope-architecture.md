# Metastore Scope Architecture Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `metastore_id` a first-class logical catalog authority scope while preserving the current `tenant + workspace` path model through a compatibility alias.

**Architecture:** Separate governed catalog authority from workspace execution context. Add a metastore-aware request and storage scope, default `metastore_id` to `workspace_id` during migration, then move catalog-governance object families onto metastore-scoped ledgers, projections, and pointer publication. Workspaces access shared metastore state only through explicit workspace bindings.

**Tech Stack:** Rust, Protobuf/prost, Arrow/Parquet, object-store CAS, `arco-core::ScopedStorage`, `arco-catalog`, `arco-api`, `arco-uc`, mdBook, `cargo test`.

---

## Status

Planning artifact created on 2026-05-12. No runtime implementation is included
in this plan document. Current code still primarily scopes storage through
`tenant={tenant}/workspace={workspace}/`.

## Non-Negotiable Invariants

- `metastore_id` is a logical authority scope, not a display name.
- `workspace_id` remains an execution and request context.
- Stable object IDs remain enforcement keys; names remain mutable aliases.
- Existing deployments can run with `metastore_id = workspace_id` until
  migration tooling exists.
- Hot paths use known keys, manifests, and pointer-published projections, not
  correctness-critical object-store listing.
- Authorization checks both object privilege and workspace binding.
- Credential vending scope is constrained by object privilege, workspace binding,
  storage governance, provider limits, and TTL policy.
- System tables remain read-only, redacted, watermarked projections and never
  become enforcement inputs.

## Target Scope Model

```text
tenant
  metastore
    catalog authority
    stable object IDs
    grants and compiled permissions
    storage governance
    catalog/search/lineage/governance/access projections
  workspace
    execution context
    orchestration state
    runtime request IDs
    binding to one or more metastores
```

## Task 0: Keep The Architecture Contract Discoverable

**Files:**
- Create: `docs/guide/src/reference/metastore-scope-architecture.md`
- Modify: `docs/guide/src/SUMMARY.md`
- Modify: `docs/guide/src/reference/documentation-map.md`

**Step 1: Add the guide page**

Document:

- the `tenant -> metastore -> catalog authority` model
- the `tenant -> workspace -> execution context` model
- the compatibility alias `metastore_id = workspace_id`
- metastore-scoped versus workspace-scoped state
- workspace binding semantics
- read, mutation, credential vending, and system-table behavior
- migration phases and open questions

**Step 2: Add the page to mdBook**

Add `Metastore Scope Architecture` under Reference in `docs/guide/src/SUMMARY.md`.

**Step 3: Add the page to the documentation map**

Add the page under `Architecture and Design` in
`docs/guide/src/reference/documentation-map.md`.

**Step 4: Verify docs**

Run: `cd docs/guide && mdbook build`

Expected: build completes without broken links.

**Step 5: Commit**

```bash
git add docs/guide/src/reference/metastore-scope-architecture.md docs/guide/src/SUMMARY.md docs/guide/src/reference/documentation-map.md
git commit -m "docs: define metastore scope architecture"
```

## Task 1: Add A Durable Scope Contract

**Files:**
- Modify: `proto/arco/catalog/v1/metastore.proto`
- Modify: `proto/arco/controlplane/v1/transactions.proto`
- Modify: `crates/arco-proto/tests/control_plane_transactions.rs`
- Create: `crates/arco-catalog/tests/metastore_scope_contracts.rs`

**Step 1: Write contract tests first**

Add tests that assert metastore-aware mutations can carry:

- `tenant_id`
- `workspace_id`
- `metastore_id`
- `request_id`
- stable object ID

The tests should prove that omitting `metastore_id` still supports the
compatibility mapping to `workspace_id` at the adapter layer, not by storing an
empty durable scope.

**Step 2: Extend protobuf additively**

Only add fields. Do not rename fields, remove fields, change field numbers, or
change existing ProtoJSON names.

Candidate additive message:

```protobuf
message CatalogControlPlaneScope {
  string tenant_id = 1;
  string workspace_id = 2;
  string metastore_id = 3;
  string request_id = 4;
}
```

Reference this scope from future metastore mutation envelopes rather than
duplicating string fields across every object message.

**Step 3: Validate compatibility**

Run: `buf lint proto/`

Expected: PASS.

Run: `cargo xtask proto-breaking-check`

Expected: PASS.

Run: `cargo test -p arco-proto --test control_plane_transactions -- --nocapture`

Expected: PASS.

**Step 4: Commit**

```bash
git add proto/arco/catalog/v1/metastore.proto proto/arco/controlplane/v1/transactions.proto crates/arco-proto/tests/control_plane_transactions.rs crates/arco-catalog/tests/metastore_scope_contracts.rs
git commit -m "feat: add metastore scope contract"
```

## Task 2: Introduce A Storage Scope Abstraction

**Files:**
- Create: `crates/arco-core/src/control_plane_scope.rs`
- Modify: `crates/arco-core/src/lib.rs`
- Modify: `crates/arco-core/src/scoped_storage.rs`
- Create: `crates/arco-core/tests/control_plane_scope.rs`

**Step 1: Write path tests**

Add tests for:

- existing workspace-scoped prefix:
  `tenant=acme/workspace=prod/`
- target metastore-scoped prefix:
  `tenant=acme/metastore=lakehouse-prod/`
- compatibility alias:
  `ControlPlaneScope::workspace_alias("acme", "prod")`
  yields `metastore_id == "prod"`
- ID validation rejects path traversal and empty IDs

**Step 2: Add the scope type**

Create a small type that records tenant, workspace, and metastore explicitly:

```rust
pub struct ControlPlaneScope {
    tenant_id: String,
    workspace_id: String,
    metastore_id: String,
}
```

Expose helpers for workspace-scoped and metastore-scoped prefixes. Keep existing
`ScopedStorage::new(tenant, workspace)` behavior intact.

**Step 3: Add metastore-scoped storage construction**

Add a constructor or wrapper that lets catalog code request metastore-scoped
storage without breaking workspace-scoped orchestration callers.

**Step 4: Run focused tests**

Run: `cargo test -p arco-core control_plane_scope -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-core scoped_storage -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-core/src/control_plane_scope.rs crates/arco-core/src/lib.rs crates/arco-core/src/scoped_storage.rs crates/arco-core/tests/control_plane_scope.rs
git commit -m "feat: add metastore-aware control plane scope"
```

## Task 3: Thread Metastore Scope Through Catalog APIs

**Files:**
- Modify: `crates/arco-catalog/src/writer.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/tier1_compactor.rs`
- Modify: `crates/arco-catalog/src/metastore/envelope.rs`
- Modify: `crates/arco-catalog/src/metastore/events.rs`
- Create: `crates/arco-catalog/tests/metastore_scope_replay.rs`

**Step 1: Write replay and path tests**

Add tests proving:

- two workspaces bound to the same metastore resolve the same stable object ID
- two different metastores can contain the same catalog/schema/table names
  without colliding
- replay digest includes the metastore scope where needed for durability
- existing workspace-alias behavior remains unchanged

**Step 2: Add scope to catalog domain APIs**

Thread `ControlPlaneScope` or an equivalent catalog-local scope through reader,
writer, and compactor construction. Keep existing constructors as compatibility
wrappers that set `metastore_id = workspace_id`.

**Step 3: Preserve current behavior**

Do not move existing paths in this task. The purpose is to make scope explicit
without changing durable storage layout yet.

**Step 4: Run focused tests**

Run: `cargo test -p arco-catalog metastore_scope -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-catalog --test protocol_invariants -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-catalog/src/writer.rs crates/arco-catalog/src/reader.rs crates/arco-catalog/src/tier1_compactor.rs crates/arco-catalog/src/metastore crates/arco-catalog/tests/metastore_scope_replay.rs
git commit -m "feat: thread metastore scope through catalog state"
```

## Task 4: Add Workspace Binding State

**Files:**
- Modify: `crates/arco-catalog/src/metastore/events.rs`
- Modify: `crates/arco-catalog/src/metastore/replay.rs`
- Modify: `crates/arco-catalog/src/metastore/projections.rs`
- Create: `crates/arco-catalog/src/storage_governance/bindings.rs`
- Create: `crates/arco-catalog/tests/workspace_bindings.rs`
- Modify: `crates/arco-catalog/tests/schema_contracts.rs`
- Create: `crates/arco-catalog/tests/golden_schemas/workspace_bindings.schema.json`

**Step 1: Write binding replay tests**

Cover active, deleted, disabled, stale, and missing bindings. Include tests for:

- workspace bound to metastore
- workspace bound to object
- no binding denies
- object belongs to different metastore denies

**Step 2: Add replay state**

Fold `WorkspaceBinding` records into metastore state keyed by stable binding ID.
Add secondary indexes by workspace ID and object ID.

**Step 3: Add projection schema**

Create `workspace_bindings.parquet` with additive fields:

- `schema_version`
- `ledger_watermark`
- `binding_id`
- `tenant_id`
- `metastore_id`
- `workspace_id`
- `object_id`
- `object_type`
- `lifecycle_state`
- `created_at`
- `updated_at`

Do not include secret material or raw provider policy internals.

**Step 4: Run focused tests**

Run: `cargo test -p arco-catalog --test workspace_bindings -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-catalog --test schema_contracts workspace_bindings -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-catalog/src/metastore crates/arco-catalog/src/storage_governance/bindings.rs crates/arco-catalog/tests/workspace_bindings.rs crates/arco-catalog/tests/schema_contracts.rs crates/arco-catalog/tests/golden_schemas/workspace_bindings.schema.json
git commit -m "feat: add workspace binding projection"
```

## Task 5: Enforce Binding In Authorization And Credential Vending

**Files:**
- Modify: `crates/arco-catalog/src/authz/decision.rs`
- Modify: `crates/arco-catalog/src/authz/explain.rs`
- Modify: `crates/arco-catalog/src/storage_governance/mod.rs`
- Modify: `crates/arco-uc/src/routes/credentials.rs`
- Modify: `crates/arco-uc/src/routes/permissions.rs`
- Create: `crates/arco-catalog/tests/workspace_binding_authz.rs`
- Create: `crates/arco-uc/tests/workspace_binding_enforcement.rs`

**Step 1: Write deny-path tests**

Add tests where a principal has object privilege but the workspace is not bound.
Expected result: deny with a stable reason such as
`workspace_binding_required`.

**Step 2: Add binding evidence to authorization decisions**

Extend authorization evidence with:

- binding ID
- binding object ID
- binding lifecycle state
- binding projection watermark

**Step 3: Enforce before credential minting**

Credential vending must deny when the workspace binding is absent, disabled,
deleted, stale, or broader/narrower than the requested object/path requires.

**Step 4: Run focused tests**

Run: `cargo test -p arco-catalog workspace_binding_authz -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-uc workspace_binding_enforcement -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-catalog/src/authz crates/arco-catalog/src/storage_governance crates/arco-uc/src/routes/credentials.rs crates/arco-uc/src/routes/permissions.rs crates/arco-catalog/tests/workspace_binding_authz.rs crates/arco-uc/tests/workspace_binding_enforcement.rs
git commit -m "feat: enforce workspace bindings for catalog access"
```

## Task 6: Move New Metastore Object Families To Metastore-Scoped Storage

**Files:**
- Modify: `crates/arco-catalog/src/metastore/publish.rs`
- Modify: `crates/arco-catalog/src/tier1_snapshot.rs`
- Modify: `crates/arco-catalog/src/tier1_compactor.rs`
- Modify: `crates/arco-core/src/catalog_paths.rs`
- Create: `crates/arco-catalog/tests/metastore_scoped_paths.rs`

**Step 1: Write path contract tests**

Assert that new metastore object-family projections publish under:

```text
tenant={tenant}/metastore={metastore}/snapshots/...
tenant={tenant}/metastore={metastore}/manifests/...
tenant={tenant}/metastore={metastore}/ledger/...
```

Keep existing catalog DDL path tests green until a dedicated migration moves
legacy catalog state.

**Step 2: Add path helpers**

Add metastore-aware path helpers through the central path module. Do not
hardcode path strings in writers or compactors.

**Step 3: Publish new projections under metastore scope**

Use metastore-scoped storage for access, storage, governance, and future
metastore object-family projections.

**Step 4: Run focused tests**

Run: `cargo test -p arco-catalog metastore_scoped_paths -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-catalog --test path_contracts -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-core/src crates/arco-catalog/src/metastore crates/arco-catalog/src/tier1_snapshot.rs crates/arco-catalog/src/tier1_compactor.rs crates/arco-catalog/tests/metastore_scoped_paths.rs
git commit -m "feat: publish metastore object projections by metastore scope"
```

## Task 7: Add System Table Visibility For Bindings

**Files:**
- Modify: `docs/guide/src/reference/system-catalog.md`
- Modify: `crates/arco-api/src/routes/query.rs`
- Modify: `crates/arco-api/src/system_tables.rs`
- Create: `crates/arco-api/tests/system_storage_workspace_bindings.rs`

**Step 1: Write system-table tests**

Add tests proving:

- unbound workspaces cannot see binding rows for unrelated metastores
- bound workspaces see only redacted binding rows
- every response includes or can derive projection freshness/watermark metadata

**Step 2: Register the table only after projection exists**

Expose:

```text
system.storage.workspace_bindings
```

Do not expose raw ledger files, raw manifests, or non-redacted policy internals.

**Step 3: Update docs**

Move `system.storage.workspace_bindings` from planned to implemented only when
the projection and tests exist.

**Step 4: Run focused tests**

Run: `cargo test -p arco-api system_storage_workspace_bindings -- --nocapture`

Expected: PASS.

Run: `cd docs/guide && mdbook build`

Expected: PASS.

**Step 5: Commit**

```bash
git add docs/guide/src/reference/system-catalog.md crates/arco-api/src crates/arco-api/tests/system_storage_workspace_bindings.rs
git commit -m "feat: expose workspace bindings system table"
```

## Task 8: Plan Legacy Catalog State Migration

**Files:**
- Create: `docs/runbooks/metastore-scope-migration.md`
- Create: `crates/arco-catalog/tests/metastore_scope_migration.rs`
- Create or modify: migration command under `tools/xtask` if migration tooling exists there

**Step 1: Document migration states**

Define:

- pre-migration workspace-scoped catalog state
- alias mode with `metastore_id = workspace_id`
- dual-published or backfilled metastore-scoped state
- cutover mode
- rollback mode

**Step 2: Add dry-run checks**

Migration tooling must report:

- source workspace scope
- target metastore scope
- object counts by family
- path conflicts
- latest ledger watermark
- projected snapshot version
- irreversible steps, if any

**Step 3: Add replay-equivalence tests**

Backfilled metastore-scoped projections must replay to the same typed state as
the source workspace-scoped projections for migrated object families.

**Step 4: Run focused tests**

Run: `cargo test -p arco-catalog metastore_scope_migration -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add docs/runbooks/metastore-scope-migration.md crates/arco-catalog/tests/metastore_scope_migration.rs tools/xtask
git commit -m "feat: add metastore scope migration plan"
```

## Full Verification Before Cutover

Run before any branch is considered merge-ready:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo test --workspace --doc
buf lint proto/
cargo xtask proto-breaking-check
cargo xtask adr-check
cd docs/guide && mdbook build
```

Expected: all commands pass. If a command is not available in the local
environment, record the exact command, failure, and reason in the handoff.

## Open Decisions To Settle Before Runtime Work

- Whether a workspace can bind to multiple metastores in the first public
  release.
- Whether principals are tenant-scoped globally or metastore-scoped with tenant
  identity federation.
- Whether system-table queries default to one active metastore or can union
  across all metastores bound to a workspace.
- Whether managed roots are always metastore-scoped or can be workspace-local
  delegations.
- The exact existence-privacy response when an object exists but the workspace
  is not bound to its metastore.
