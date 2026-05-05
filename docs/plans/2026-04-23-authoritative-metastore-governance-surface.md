# Authoritative Metastore Governance Surface Implementation Plan

**Goal:** Extend Arco's authoritative control plane beyond catalog/schema/table CRUD so the missing Unity Catalog-style metastore domains become real repo-owned state: grants/RBAC, storage credentials and bindings, governance metadata, and non-table object families such as volumes, functions, and model registry objects.

**Architecture:** Reuse the existing Arco pattern instead of introducing a separate metastore database. Add stable-ID metastore objects and operations that append to immutable control-plane state, compact into manifest-selected Parquet projections, and publish through fenced pointer CAS. Keep the UC facade, temporary credential vending, root-pinned reads, and future system tables serving from published projections or derived compiled views, never from preview JSON blobs, placeholder payloads, or object-store listing.

**Tech Stack:** Rust, Protobuf/prost, Axum, Arrow/Parquet, `arco-catalog`, `arco-uc`, `arco-api`, mdBook, `cargo test`.

---

## Repo-Grounded Gap Summary

The current repo has moved past "basic catalog CRUD is missing" as the primary gap.
The remaining missing pieces are the surrounding metastore and governance domains
that make a UC-like metastore authoritative rather than parity-shaped.

- `crates/arco-uc/src/routes/permissions.rs` still returns empty
  `privilege_assignments` on `GET`, and `PATCH` is explicitly unsupported.
- `crates/arco-uc/src/routes/credentials.rs` still exposes placeholder or
  unsupported temporary credential behavior rather than authoritative storage
  credential, external location, or binding state.
- `docs/guide/src/reference/control-plane-scope.md` still marks grants/RBAC,
  credentials/external locations, governance rules, and ownership/tags as
  `Planned`.
- `proto/arco/catalog/v1/catalog.proto` still models only catalog/schema/table
  DDL; there is no authoritative protobuf contract yet for the broader
  metastore/governance object model.
- `crates/arco-uc/tests/fixtures/unitycatalog-openapi.yaml` and
  `docs/guide/src/reference/unity-catalog-openapi-inventory.md` still describe
  additional UC object families and governance surfaces that Arco does not yet
  own authoritatively.
- Some governance-like values already exist as free-form `properties` or asset
  metadata, but that is not the same thing as first-class authoritative state
  with stable IDs, mutation semantics, publication contracts, and queryable
  projections.

## Scope

- Introduce authoritative state and mutation paths for grants/RBAC.
- Introduce authoritative state and mutation paths for storage credentials,
  external locations, workspace/storage bindings, and truthful temporary
  credential vending.
- Introduce authoritative governance state for ownership, tags/labels,
  classification, masking policy attachment, and related control bindings.
- Introduce authoritative UC-style object families beyond tables:
  - volumes
  - functions/UDFs
  - registered models
  - model versions
- Extend the existing ledger -> compaction -> immutable snapshot -> pointer
  publication model across the broader metastore/governance surface.
- Preserve system tables and audit projections as read-only derived consumers of
  this state, not the source of truth.

## Non-Goals

- Do not add an always-on OLTP metastore.
- Do not make policy evaluation, temp credential vending, or discovery depend on
  raw ledger scans or correctness-critical object-store listing.
- Do not treat free-form `properties` maps as a substitute for typed,
  authoritative governance state.
- Do not try to reach full Unity Catalog feature parity in one tranche; the
  priority is authoritative state plus mutation semantics.
- Do not put derived SQL tables or audit/event projections into the synchronous
  enforcement path.

## Recommended Delivery Order

1. Lock the contract in docs and ADRs.
2. Add durable metastore/governance contracts with stable IDs and snapshot
   projections.
3. Land grants/RBAC and make `/permissions/...` authoritative.
4. Land storage credentials, external locations, bindings, and truthful
   temporary credential vending.
5. Land governance attachments and compiled read views over ownership,
   classification, and policy state.
6. Land volumes/functions/models on the same published path.
7. Expose the new projections to root-pinned reads, system tables, and
   governance/audit follow-ons.

## Verification Matrix

- `cargo test -p arco-catalog --test schema_contracts -- --nocapture`
- `cargo test -p arco-catalog --test protocol_invariants -- --nocapture`
- `cargo test -p arco-uc --test discovery_endpoints -- --nocapture`
- `cargo test -p arco-uc --test preview_crud -- --nocapture`
- `cargo test -p arco-uc --test openapi_compliance -- --nocapture`
- `cargo test -p arco-api --test system_tables_api -- --nocapture`
- `cargo fmt --all --check`
- `cargo clippy -p arco-catalog -p arco-uc -p arco-api --tests -- -D warnings`
- `cargo xtask adr-check`
- `cd docs/guide && mdbook build`

## Acceptance Criteria

- Permissions/grants are backed by published authoritative state, not empty
  scaffolding or ad hoc responses.
- Temporary credential responses are derived from authoritative storage
  credential, external location, and binding state, with auditable allow/deny
  decisions.
- Ownership/tags/classification/policy attachments are represented as
  first-class control-plane state with stable IDs and clear mutation semantics,
  not only opaque `properties`.
- Volumes, functions, registered models, and model versions have authoritative
  CRUD/read surfaces and published snapshot projections.
- The broader metastore/governance surface follows the same visibility boundary
  as catalog/orchestration today: immutable writes plus fenced head publication.
- The guide and control-plane scope docs can move these domains from `Planned`
  to `Partial`, then `Implemented`, with executable proof.

### Task 1: Lock The Contract In Docs

**Files:**
- Create: `docs/adr/adr-036-authoritative-metastore-governance-surface.md`
- Modify: `docs/guide/src/reference/control-plane-scope.md`
- Modify: `docs/guide/src/reference/unity-catalog-openapi-inventory.md`
- Modify: `docs/guide/src/concepts/catalog.md`
- Modify: `docs/guide/src/concepts/architecture.md`

**Step 1: Write the ADR**

Capture the repo-grounded decision that Arco will extend the same authoritative
publish model across grants, credentials/bindings, governance attachments, and
additional UC object families. The ADR should lock these boundaries:

- truth remains immutable control-plane state plus published manifests
- temp credential vending and policy checks serve from published state
- system tables and audit streams are derived projections
- free-form metadata maps are compatibility shims, not authoritative governance

**Step 2: Update scope and inventory docs**

Update the control-plane scope page so it distinguishes:

- grants/RBAC
- storage credentials and external locations
- governance attachments
- volumes/functions/models

Update the Unity Catalog inventory docs so parity-shaped OpenAPI inventory is
not mistaken for implemented authoritative runtime support.

**Step 3: Run doc verification**

Run: `cargo xtask adr-check`
Expected: `SUCCESS`

Run: `cd docs/guide && mdbook build`
Expected: build completes without broken links

**Step 4: Commit**

```bash
git add docs/adr/adr-036-authoritative-metastore-governance-surface.md docs/guide/src/reference/control-plane-scope.md docs/guide/src/reference/unity-catalog-openapi-inventory.md docs/guide/src/concepts/catalog.md docs/guide/src/concepts/architecture.md
git commit -m "docs: define authoritative metastore governance surface"
```

### Task 2: Add Stable-ID Metastore Contracts And Snapshot Projections

**Files:**
- Create: `proto/arco/catalog/v1/metastore.proto`
- Create: `crates/arco-catalog/src/metastore_events.rs`
- Create: `crates/arco-catalog/src/metastore_state.rs`
- Create: `crates/arco-catalog/src/metastore_snapshot.rs`
- Modify: `crates/arco-catalog/src/lib.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Test: `crates/arco-catalog/tests/schema_contracts.rs`
- Test: `crates/arco-catalog/tests/protocol_invariants.rs`

**Step 1: Write the failing catalog contract tests**

Add failing tests that prove the current catalog domain does not yet materialize
or publish metastore/governance projections such as:

- `grants.parquet`
- `storage_credentials.parquet`
- `external_locations.parquet`
- `workspace_bindings.parquet`
- `governance_attachments.parquet`
- `volumes.parquet`
- `functions.parquet`
- `registered_models.parquet`
- `model_versions.parquet`

Also add failures that prove stable IDs survive rename-based lookup changes.

**Step 2: Add a dedicated metastore protobuf**

`metastore.proto` is part of the final pre-freeze `arco.catalog.v1` hard cut.

Do not keep inflating `catalog.proto` until it becomes unreadable. Add a new
versioned contract for metastore/governance state with stable IDs and explicit
operations for:

- grants and privilege mutations
- storage credentials
- external locations
- workspace or storage bindings
- governance attachments
- volumes
- functions
- registered models
- model versions

Reserve forward-compatibility fields rather than overloading free-form maps.

**Step 3: Add catalog-side state/snapshot modules**

Introduce dedicated `metastore_*` modules in `arco-catalog` so the new domains
do not further bloat the existing catalog table writer path. Extend the
compaction flow so published catalog snapshots include the new Parquet
projections via manifest-selected paths and keep readers pointer-first.

**Step 4: Run focused contract tests**

Run: `cargo test -p arco-catalog --test schema_contracts -- --nocapture`
Expected: PASS with the new projection names and stable-ID invariants covered

Run: `cargo test -p arco-catalog --test protocol_invariants -- --nocapture`
Expected: PASS with no ledger-scan or list-dependent correctness regressions

**Step 5: Commit**

```bash
git add proto/arco/catalog/v1/metastore.proto crates/arco-catalog/src/metastore_events.rs crates/arco-catalog/src/metastore_state.rs crates/arco-catalog/src/metastore_snapshot.rs crates/arco-catalog/src/lib.rs crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-catalog/tests/schema_contracts.rs crates/arco-catalog/tests/protocol_invariants.rs
git commit -m "feat: add authoritative metastore state contracts"
```

### Task 3: Implement Authoritative Grants And Permissions

**Files:**
- Modify: `crates/arco-uc/src/routes/permissions.rs`
- Modify: `crates/arco-uc/src/openapi.rs`
- Modify: `crates/arco-uc/src/router.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Test: `crates/arco-uc/tests/discovery_endpoints.rs`
- Create: `crates/arco-uc/tests/permissions_authoritative.rs`

**Step 1: Write the failing permissions tests**

Add failing tests that require:

- `GET /permissions/{securable_type}/{full_name}` returns compiled assignments
  from published state
- `PATCH /permissions/{securable_type}/{full_name}` durably applies grant and
  revoke changes
- the optional `principal` query filters authoritative assignments instead of
  always returning an empty list
- bootstrap workspace behavior follows ADR-030's first-manager semantics

**Step 2: Add writer and reader support for grants**

Add minimal Arco-side grant operations and compiled read APIs keyed by stable
object ID rather than mutable names. Renames must preserve grant bindings.

**Step 3: Make the UC permissions route authoritative**

Replace the scaffolded handler so:

- `GET` reads the compiled assignments from manifest-selected state
- `PATCH` validates the request, appends authoritative mutations, and returns
  the resulting privilege assignments

Keep the route read-only with respect to derived projections. The write path
must still be ledger append plus compaction plus publish.

**Step 4: Run focused tests**

Run: `cargo test -p arco-uc --test permissions_authoritative -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-uc --test discovery_endpoints -- --nocapture`
Expected: PASS with the old scaffold-only assumptions removed or updated

**Step 5: Commit**

```bash
git add crates/arco-uc/src/routes/permissions.rs crates/arco-uc/src/openapi.rs crates/arco-uc/src/router.rs crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-uc/tests/discovery_endpoints.rs crates/arco-uc/tests/permissions_authoritative.rs
git commit -m "feat: add authoritative grants and permissions"
```

### Task 4: Implement Authoritative Storage Credentials, External Locations, And Bindings

**Files:**
- Create: `crates/arco-uc/src/routes/storage_credentials.rs`
- Create: `crates/arco-uc/src/routes/external_locations.rs`
- Modify: `crates/arco-uc/src/routes/credentials.rs`
- Modify: `crates/arco-uc/src/router.rs`
- Modify: `crates/arco-uc/src/openapi.rs`
- Modify: `crates/arco-uc/src/audit.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Create: `crates/arco-uc/tests/storage_credentials_authoritative.rs`
- Create: `crates/arco-uc/tests/external_locations_authoritative.rs`

**Step 1: Write the failing storage and temp-credential tests**

Add failing tests for:

- create/get/list/update/delete storage credentials
- create/get/list/update/delete external locations
- workspace or storage bindings that control visibility and usage
- truthful `temporary-table-credentials`, `temporary-volume-credentials`,
  `temporary-model-version-credentials`, and `temporary-path-credentials`
  responses driven by authoritative state

**Step 2: Add durable object and binding state**

Model storage credentials, external locations, and binding edges as
authoritative objects with stable IDs and manifest-published projections. Keep
cloud-specific vending material derived from those objects and the current authz
decision, not as the source of truth itself.

**Step 3: Replace placeholder temporary credential behavior**

The temporary credential endpoints should stop returning placeholder empty
credential lists or blanket `501`s for supported cases. They should:

- load the referenced table, volume, model, or path binding from published state
- evaluate the authoritative allow/deny decision
- emit audit events for allow/deny
- return a truthful backend-specific credential or signed-access payload when
  supported, or a truthful explicit denial when not allowed

**Step 4: Run focused tests**

Run: `cargo test -p arco-uc --test storage_credentials_authoritative -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-uc --test external_locations_authoritative -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-uc --test openapi_compliance -- --nocapture`
Expected: PASS with the OpenAPI tags no longer describing these routes as
non-authoritative scaffolding

**Step 5: Commit**

```bash
git add crates/arco-uc/src/routes/storage_credentials.rs crates/arco-uc/src/routes/external_locations.rs crates/arco-uc/src/routes/credentials.rs crates/arco-uc/src/router.rs crates/arco-uc/src/openapi.rs crates/arco-uc/src/audit.rs crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-uc/tests/storage_credentials_authoritative.rs crates/arco-uc/tests/external_locations_authoritative.rs
git commit -m "feat: add authoritative storage credentials and bindings"
```

### Task 5: Make Governance Metadata First-Class Control-Plane State

**Files:**
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Modify: `crates/arco-uc/src/routes/catalogs.rs`
- Modify: `crates/arco-uc/src/routes/schemas.rs`
- Modify: `crates/arco-uc/src/routes/tables.rs`
- Create: `crates/arco-uc/tests/governance_metadata_authoritative.rs`

**Step 1: Write the failing governance metadata tests**

Add tests that prove the repo currently lacks authoritative semantics for:

- owners
- tags or labels
- classification
- masking or policy attachment references

The tests should require these bindings to survive rename operations because the
binding key is the stable object ID, not the mutable display name.

**Step 2: Add typed governance attachments**

Do not continue treating governance state as opaque free-form `properties`
entries only. Add typed attachment rows or bindings keyed by object ID and
attachment type. Keep compatibility by projecting supported typed values back
into UC-facing `properties` when needed.

**Step 3: Wire existing CRUD routes through the authoritative fields**

Catalog, schema, and table PATCH handlers should become the compatibility surface
over first-class governance state instead of being the only place those values
exist.

**Step 4: Run focused tests**

Run: `cargo test -p arco-uc --test governance_metadata_authoritative -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-uc --test preview_crud -- --nocapture`
Expected: PASS with old metadata round-trip assertions updated to cover
authoritative typed bindings

**Step 5: Commit**

```bash
git add crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-uc/src/routes/catalogs.rs crates/arco-uc/src/routes/schemas.rs crates/arco-uc/src/routes/tables.rs crates/arco-uc/tests/governance_metadata_authoritative.rs crates/arco-uc/tests/preview_crud.rs
git commit -m "feat: make governance metadata authoritative"
```

### Task 6: Add Authoritative UC Object Families Beyond Tables

**Files:**
- Create: `crates/arco-uc/src/routes/volumes.rs`
- Create: `crates/arco-uc/src/routes/functions.rs`
- Create: `crates/arco-uc/src/routes/models.rs`
- Modify: `crates/arco-uc/src/routes/mod.rs`
- Modify: `crates/arco-uc/src/router.rs`
- Modify: `crates/arco-uc/src/openapi.rs`
- Modify: `crates/arco-uc/src/contract.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Create: `crates/arco-uc/tests/volumes_authoritative.rs`
- Create: `crates/arco-uc/tests/functions_authoritative.rs`
- Create: `crates/arco-uc/tests/models_authoritative.rs`

**Step 1: Write the failing object-family tests**

Add failing CRUD and read tests for:

- volumes
- functions
- registered models
- model versions

Keep the first tranche deliberately narrow: authoritative create/get/list/update
and delete semantics, not every optional UC field on day one.

**Step 2: Add stable-ID object support in the metastore state**

Each object family should follow the same pattern as existing catalog/schema/table
state:

- stable ID
- immutable mutation events
- compactor-owned Parquet projection
- published manifest-selected visibility

**Step 3: Add UC route modules and OpenAPI coverage**

Add explicit route modules instead of hiding new surfaces behind generic preview
helpers. The OpenAPI tags should distinguish authoritative surfaces from any
remaining scaffolding.

**Step 4: Run focused tests**

Run: `cargo test -p arco-uc --test volumes_authoritative -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-uc --test functions_authoritative -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-uc --test models_authoritative -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add crates/arco-uc/src/routes/volumes.rs crates/arco-uc/src/routes/functions.rs crates/arco-uc/src/routes/models.rs crates/arco-uc/src/routes/mod.rs crates/arco-uc/src/router.rs crates/arco-uc/src/openapi.rs crates/arco-uc/src/contract.rs crates/arco-catalog/src/reader.rs crates/arco-catalog/src/writer.rs crates/arco-uc/tests/volumes_authoritative.rs crates/arco-uc/tests/functions_authoritative.rs crates/arco-uc/tests/models_authoritative.rs
git commit -m "feat: add authoritative UC object families"
```

### Task 7: Generalize Metastore-Wide Reads And Visibility Follow-Ons

**Files:**
- Modify: `proto/arco/controlplane/v1/transactions.proto`
- Modify: `crates/arco-api/src/control_plane_transactions.rs`
- Modify: `crates/arco-api/src/system_tables.rs`
- Modify: `crates/arco-api/src/routes/query.rs`
- Test: `crates/arco-api/tests/root_transaction_protocol.rs`
- Test: `crates/arco-api/tests/system_tables_api.rs`

**Step 1: Write the failing read-surface tests**

Add failing tests that require:

- root transactions to pin the wider metastore projections, not only the current
  catalog subset
- `/api/v1/query` to expose the new allowlisted system projections once they
  exist

The first system-table tranche should stay small and obvious, for example:

- `system.access.grants`
- `system.access.storage_credentials`
- `system.access.external_locations`
- `system.catalog.volumes`
- `system.catalog.functions`
- `system.catalog.registered_models`

**Step 2: Extend the read contract without changing the truth boundary**

Published metastore projections should become part of the pinned read and
queryable operations surface, but the truth boundary must remain the immutable
write plus pointer publication contract from the earlier tasks.

**Step 3: Run focused tests**

Run: `cargo test -p arco-api --test root_transaction_protocol -- --nocapture`
Expected: PASS

Run: `cargo test -p arco-api --test system_tables_api -- --nocapture`
Expected: PASS with the new allowlisted metastore tables

**Step 4: Commit**

```bash
git add proto/arco/controlplane/v1/transactions.proto crates/arco-api/src/control_plane_transactions.rs crates/arco-api/src/system_tables.rs crates/arco-api/src/routes/query.rs crates/arco-api/tests/root_transaction_protocol.rs crates/arco-api/tests/system_tables_api.rs
git commit -m "feat: expose authoritative metastore projections on read surfaces"
```

## Prioritized Follow-Ons

- Add `system.access.audit`, `system.access.auth_denies`, and
  `system.access.url_mint_events` after the allow/deny audit stream is projected
  into Parquet rather than remaining tracing-only.
- Add native Arco APIs for metastore-governance mutations if the UC facade stops
  being the preferred authoring surface.
- Add sharing/export surfaces only after the authoritative object and grant model
  is stable.
- Keep richer masking and policy execution engines out of the first tranche until
  the attachment state model is real and queryable.
