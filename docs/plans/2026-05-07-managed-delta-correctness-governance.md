# Managed Delta Correctness And Governance Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Arco-managed Delta tables catalog-authoritative: table identity, storage location, protocol/features, write permissions, temporary credentials, and commit coordination are governed by Arco-native control-plane state instead of ad hoc object-store behavior or UC facade assumptions.

**Architecture:** Treat Unity Catalog as prior art for the managed-table contract, not as Arco's source of truth. Arco's catalog ledger and pointer-published snapshots own table identity, location bindings, storage governance, and authorization; Delta commit APIs are adapters over that state. The Delta coordinator remains the correctness point for managed commits, while `_delta_log` files and system tables are durable artifacts/projections derived from successful coordinated commits.

**Tech Stack:** Rust, Axum, Protobuf/prost, Arrow/Parquet, object-store CAS, `arco-catalog`, `arco-delta`, `arco-api`, `arco-uc`, DataFusion system tables, `cargo test`.

---

## Product Framing

This is not a "connect Arco to UC" plan. UC is useful prior art because it models the product-level responsibilities of a best-in-class lakehouse catalog: stable object identity, governed storage, managed table commit coordination, temporary credential vending, access control, lineage, and operational metadata. Arco should implement those responsibilities natively using its own ledger, compaction, immutable manifests, and fenced pointer publication.

The core product promise is:

> When a Delta table is managed by Arco, the Arco catalog is the authority for who may write it, where it lives, what protocol/features it supports, which version is current, and how failed or duplicated commits are recovered.

## Repo-Grounded Gap Summary

- `crates/arco-api/src/routes/delta.rs` already resolves table IDs through `CatalogReader` and enforces Delta format before native commits.
- `crates/arco-uc/src/routes/delta_commits.rs` implements real coordinator-backed commit behavior, but it is not yet sufficiently bound to authoritative catalog table state.
- `crates/arco-uc/src/routes/delta_commits.rs` can discover commit state from `_delta_log` listing through a table-ID-derived path; managed Delta correctness should use known-key/coordinator state in hot paths.
- `crates/arco-uc/tests/delta_commit_coordinator_semantics.rs` tests coordinator behavior, but current coverage does not prove catalog table identity, table type, storage location, or grants are enforced.
- `crates/arco-uc/src/routes/credentials.rs` still has placeholder temporary credential behavior.
- `crates/arco-uc/src/routes/permissions.rs` still has scaffolded permission behavior.
- `docs/adr/adr-030-delta-uc-metastore.md` already states the right invariants: no correctness-critical listing, catalog-managed Delta framing, staging upload, and first-class Delta metadata projections.
- `docs/guide/src/reference/control-plane-scope.md` currently marks broader credentials, grants, and policy state as planned.

## Scope

- Define an Arco-native managed Delta table contract.
- Bind every managed Delta write API to authoritative catalog table state.
- Enforce table ID, table format, table type, storage root, and table location invariants.
- Make coordinator state the request-time correctness source for managed Delta commits.
- Keep object-store listing out of managed commit hot paths.
- Add validation for Delta protocol/features/properties required by Arco-managed tables.
- Add staging-table/finalization semantics needed for safe table creation and initial commit.
- Wire managed Delta writes through grants and storage-governance checks once those domains become authoritative.
- Add system/audit projections for managed Delta commits, stale staged commits, credential mints, denials, and repair/reconciliation events.
- Keep UC-compatible Delta endpoints as adapter surfaces over the same Arco-native invariants.

## Non-Goals

- Do not make Arco depend on a Unity Catalog deployment.
- Do not implement a general-purpose Delta query engine in this tranche.
- Do not support every Delta table feature before the core managed-table contract is safe.
- Do not make object-store listing part of the normal managed commit correctness path.
- Do not use system tables as the enforcement path; they are observability projections.
- Do not treat free-form table properties as a substitute for typed managed-table state.

## Core Invariants

- A managed Delta commit must fail if `table_id` does not resolve to an authoritative catalog table.
- A managed Delta commit must fail if the table's effective format is not `delta`.
- A managed Delta commit must fail if the table is not in a write mode that Arco manages.
- A managed Delta commit must fail if the requested table URI does not match the catalog-bound storage location.
- A managed Delta commit must fail if the caller lacks the required write privilege.
- Temporary credentials must be scoped to the table or external location that authorized the request.
- Coordinator state, not object-store listing, decides the next commit version in request-time managed write paths.
- `_delta_log/{version}.json` writes must use object-store preconditions.
- Idempotency keys must replay the original successful result and must not create duplicate commits.
- Delta protocol/features/properties accepted at creation time must be stable, validated, and queryable.
- Secret material must never be written to Delta coordinator state, `_delta_log` actions, tenant-visible projections, system tables, logs, traces, errors, or test snapshots.
- Deny decisions for managed Delta reads, writes, and credential mints must be auditable without leaking sensitive policy internals.

## Security And API Contract Dependencies

This plan depends on the catalog product Phase 0 contracts:

- `docs/adr/adr-038-catalog-threat-model.md`
- `docs/adr/adr-039-catalog-consistency-model.md`
- `docs/guide/src/reference/catalog-privilege-matrix.md`
- `docs/guide/src/reference/catalog-api-contract.md`
- `docs/guide/src/reference/schema-evolution-policy.md`
- `docs/guide/src/reference/credential-vending-security.md`

Execute `docs/plans/2026-05-07-catalog-product-surface.md` Task 0 first. If
this managed-Delta plan is executed independently before Task 0 lands, Task 1
must create these shared contract files instead of modifying missing paths.

Managed Delta APIs must follow the shared catalog API contract:

- mutating requests use idempotency keys where retries can duplicate work
- stale writes are protected by read versions, ETags, generation IDs, or protocol-specific concurrency tokens
- errors are stable and machine-readable
- response schemas distinguish ordinary, owner, and admin-visible fields
- compatibility endpoints are adapters over Arco-native state

Managed Delta authorization must use the formal `AuthzDecision` subsystem. Route
handlers must not perform bespoke permission checks for table writes, path
credentials, table credentials, system-table reads, or repair operations.

## Test Hygiene Policy

Do not commit knowingly failing default tests. During implementation, use the
TDD loop locally, but commit only after the slice is green. Future managed Delta
contract tests must be ignored with a tracking reason, feature-gated under a
non-default pending-contract feature, or captured as evidence-only assertions in
the ADR until implementation lands.

## Recommended Delivery Order

1. Phase 0: lock the shared catalog threat model, consistency model, API contract, privilege matrix, schema-evolution policy, and credential-vending security contract.
2. Add catalog-bound table resolution helpers shared by native and UC Delta routes.
3. Remove object-store listing from managed Delta request-time correctness paths.
4. Enforce managed-table creation and protocol/property rules.
5. Add staging/finalization for create-table and first-commit flows.
6. Integrate `AuthzDecision`, grants, credential vending, and audit once the broader catalog surface lands.
7. Publish Delta operational projections and expose them through ACL-protected system tables.
8. Add repair/reconciliation tooling for coordinator/log drift.
9. Add compatibility fixtures for Delta clients that exercise UC-compatible and Arco-native request/response behavior.

## Verification Matrix

- `cargo test -p arco-api --test delta_managed_table_semantics -- --nocapture`
- `cargo test -p arco-uc --test delta_commit_coordinator_semantics -- --nocapture`
- `cargo test -p arco-uc --test delta_catalog_binding -- --nocapture`
- `cargo test -p arco-catalog --test schema_contracts -- --nocapture`
- `cargo test -p arco-catalog --test protocol_invariants -- --nocapture`
- `cargo test -p arco-api --test system_tables_api -- --nocapture`
- `cargo test -p arco-uc --test permissions_authoritative -- --nocapture`
- `cargo test -p arco-uc --test credentials_authoritative -- --nocapture`
- `cargo fmt --all --check`
- `cargo clippy -p arco-api -p arco-uc -p arco-delta -p arco-catalog --tests --all-features -- -D warnings`
- `cargo xtask adr-check`
- `cargo xtask verify-integrity`
- `cargo xtask repo-hygiene-check`
- `cargo xtask engine-boundary-check`
- `cargo xtask parity-matrix-check`
- `cd docs/guide && mdbook build`

Future managed-Delta gates must be implemented in `tools/xtask` before they are
added to the required verification matrix or referenced as runnable commands:

- schema compatibility check
- OpenAPI diff check
- redaction check
- projection replay check
- compatibility fixture check

## Acceptance Criteria

- UC and Arco-native Delta commit APIs reject arbitrary UUIDs that do not resolve to catalog tables.
- UC and Arco-native Delta commit APIs share the same table resolution, format, location, and managed-mode validation.
- Managed Delta request-time commit correctness does not depend on object-store `list`.
- Managed Delta table creation persists typed protocol/features/properties needed to govern future commits.
- Temporary table/path credentials are only issued after table, location, grant, and operation checks pass.
- Managed Delta commit events and repair state are visible through ACL-protected read-only projections/system tables.
- The docs describe UC as prior art and compatibility surface, not as an architectural dependency.
- Managed Delta credential scopes are provably subsets of the governed table, volume, or path decision.
- Provider failures, ambiguous paths, stale permissions, and stale projections deny closed with audit.
- Compatibility endpoints have golden request/response fixtures.

### Task 1: Lock The Arco Managed Delta Contract

**Files:**
- Create: `docs/adr/adr-040-managed-delta-catalog-governance.md`
- Modify: `docs/adr/README.md`
- Modify: `docs/adr/adr-030-delta-uc-metastore.md`
- Modify after catalog-product Task 0, or create if executing independently: `docs/adr/adr-038-catalog-threat-model.md`
- Modify after catalog-product Task 0, or create if executing independently: `docs/adr/adr-039-catalog-consistency-model.md`
- Modify after catalog-product Task 0, or create if executing independently: `docs/guide/src/reference/catalog-api-contract.md`
- Modify after catalog-product Task 0, or create if executing independently: `docs/guide/src/reference/catalog-privilege-matrix.md`
- Modify after catalog-product Task 0, or create if executing independently: `docs/guide/src/reference/schema-evolution-policy.md`
- Modify after catalog-product Task 0, or create if executing independently: `docs/guide/src/reference/credential-vending-security.md`
- Modify: `docs/guide/src/reference/control-plane-scope.md`
- Modify: `docs/guide/src/concepts/catalog.md`
- Modify: `docs/guide/src/reference/unity-catalog-openapi-inventory.md`

**Step 1: Write the ADR**

Add a short ADR that states:

- Arco-managed Delta is catalog-authoritative.
- UC is prior art and an API compatibility reference, not a dependency.
- Catalog table ID, table type, format, and storage location are mandatory commit preconditions.
- The Delta coordinator is the request-time source for managed commit sequencing.
- `_delta_log` listing is allowed only for migration, repair, and anti-entropy tooling.
- System tables are derived observability surfaces, not enforcement points.
- Managed Delta routes call `AuthzDecision` for writes, credential mints, repair, and sensitive operational reads.
- Managed Delta credential-vending behavior follows the catalog credential-vending security reference.

**Step 2: Update existing docs**

Update ADR-030 and the control-plane scope page so they distinguish:

- implemented coordinator mechanics
- partially implemented catalog binding
- planned grants, credentials, and projections

Replace wording that makes "UC parity" sound like the product goal with wording that says UC is prior art and an interoperability target.

Update the shared threat model, consistency model, API contract, privilege
matrix, schema-evolution policy, and credential-vending security docs with
managed Delta-specific threats, operations, error codes, credential scopes, and
consistency behavior.

**Step 3: Run doc verification**

Run: `cargo xtask adr-check`

Expected: `SUCCESS`

Run: `cd docs/guide && mdbook build`

Expected: build completes without broken links.

**Step 4: Commit**

```bash
git add docs/adr/adr-040-managed-delta-catalog-governance.md docs/adr/README.md docs/adr/adr-030-delta-uc-metastore.md docs/adr/adr-038-catalog-threat-model.md docs/adr/adr-039-catalog-consistency-model.md docs/guide/src/reference/catalog-api-contract.md docs/guide/src/reference/catalog-privilege-matrix.md docs/guide/src/reference/schema-evolution-policy.md docs/guide/src/reference/credential-vending-security.md docs/guide/src/reference/control-plane-scope.md docs/guide/src/concepts/catalog.md docs/guide/src/reference/unity-catalog-openapi-inventory.md
git commit -m "docs: define arco managed delta governance contract"
```

### Task 2: Add Catalog-Bound Delta Route Tests

**Files:**
- Create: `crates/arco-api/tests/delta_managed_table_semantics.rs`
- Create: `crates/arco-uc/tests/delta_catalog_binding.rs`
- Modify: `crates/arco-uc/tests/delta_commit_coordinator_semantics.rs`
- Modify: `crates/arco-integration-tests/tests/delta_engine_smoke.rs`

**Step 1: Write native API tests**

Add tests proving `/api/v1/delta/tables/{table_id}/commits/stage` and `/api/v1/delta/tables/{table_id}/commits`:

- return `404` for an unknown `table_id`
- return `409` for a known non-Delta table
- return `409` for a Delta table that is not managed by Arco
- use the catalog table location, not a caller-supplied path
- call the shared authorization decision path for write operations
- redact sensitive values from errors and logs

During local TDD these tests may fail before implementation, but do not commit
them as default failing tests.

**Step 2: Write UC facade tests**

Add tests proving `/api/2.1/unity-catalog/delta/preview/commits`:

- rejects unknown table IDs
- rejects non-Delta tables
- rejects location mismatches between `table_uri` and catalog state
- rejects commits when the catalog table is not in managed Delta mode
- does not create coordinator/backfill state for rejected requests
- returns stable machine-readable error codes
- preserves request IDs in error responses

**Step 3: Fix the integration smoke test**

Update the Delta engine smoke test so the UC commit path uses the actual table ID returned by table creation. The test must no longer prove coordinator mechanics with a fresh unrelated UUID.

**Step 4: Run tests during TDD**

Run: `cargo test -p arco-api --test delta_managed_table_semantics -- --nocapture`

Expected during local red phase: FAIL until the route validation is implemented.

Run: `cargo test -p arco-uc --test delta_catalog_binding -- --nocapture`

Expected during local red phase: FAIL until the UC facade resolves tables through the catalog.

Do not commit at this step unless the tests are ignored with a tracking reason
or feature-gated under a non-default pending-contract feature. The preferred
path is to continue directly to Task 3 and commit the tests with the passing
implementation.

**Step 5: Defer commit until green**

No commit. Carry these test changes into Task 3.

### Task 3: Share Managed Delta Table Resolution

**Files:**
- Create: `crates/arco-api/src/delta_table_resolution.rs`
- Modify: `crates/arco-api/src/lib.rs`
- Modify: `crates/arco-api/src/routes/delta.rs`
- Modify: `crates/arco-uc/src/routes/delta_commits.rs`
- Modify: `crates/arco-uc/src/state.rs`

**Step 1: Add shared resolution helper**

Create a helper that accepts request context, storage, table ID, optional table URI, and required mode. It should:

- load the catalog table by stable ID
- normalize the effective table format
- enforce `delta`
- enforce managed Delta mode where required
- derive `DeltaPaths` from the catalog table location
- compare caller-provided URI to catalog location when a compatibility API includes URI
- return a typed resolved table object with table ID, full name, location, table type, and `DeltaPaths`

**Step 2: Wire native routes through the helper**

Replace route-local table resolution in `crates/arco-api/src/routes/delta.rs` with the shared helper. Preserve existing native API response shapes.

**Step 3: Wire UC routes through the helper**

Update `crates/arco-uc/src/routes/delta_commits.rs` so both GET and POST resolve the table before touching coordinator, staging, backfill, or log state.

**Step 4: Run focused tests**

Run: `cargo test -p arco-api --test delta_managed_table_semantics -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-uc --test delta_catalog_binding -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-api/src/delta_table_resolution.rs crates/arco-api/src/lib.rs crates/arco-api/src/routes/delta.rs crates/arco-uc/src/routes/delta_commits.rs crates/arco-uc/src/state.rs crates/arco-api/tests/delta_managed_table_semantics.rs crates/arco-uc/tests/delta_catalog_binding.rs crates/arco-uc/tests/delta_commit_coordinator_semantics.rs crates/arco-integration-tests/tests/delta_engine_smoke.rs
git commit -m "feat: bind managed delta commits to catalog tables"
```

### Task 4: Remove Listing From Managed Commit Correctness

**Files:**
- Modify: `crates/arco-uc/src/routes/delta_commits.rs`
- Modify: `crates/arco-delta/src/coordinator.rs`
- Modify: `crates/arco-delta/src/types.rs`
- Create: `crates/arco-delta/tests/no_list_hot_path.rs`
- Modify: `crates/arco-uc/tests/delta_commit_coordinator_semantics.rs`

**Step 1: Write a no-list regression test**

Add a storage test double that fails any `list` call during normal managed commit GET/POST paths. The test should allow known-key `HEAD` and `GET`, and should fail if the route calls list to compute current commit state.

**Step 2: Make coordinator state sufficient**

Extend coordinator state so managed request paths can answer:

- latest committed/reserved version
- unbackfilled commit window
- idempotency replay
- stale read conflicts
- maximum unbackfilled commit count

without scanning `_delta_log`.

**Step 3: Move listing into explicit repair helpers**

If existing list-based behavior is still useful, move it behind an explicit reconciliation/repair function that is not called from normal commit handlers.

**Step 4: Run focused tests**

Run: `cargo test -p arco-delta --test no_list_hot_path -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-uc --test delta_commit_coordinator_semantics -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-uc/src/routes/delta_commits.rs crates/arco-delta/src/coordinator.rs crates/arco-delta/src/types.rs crates/arco-delta/tests/no_list_hot_path.rs crates/arco-uc/tests/delta_commit_coordinator_semantics.rs
git commit -m "fix: remove listing from managed delta commit path"
```

### Task 5: Enforce Managed Delta Creation Rules

**Files:**
- Modify: `crates/arco-catalog/src/writer.rs`
- Modify: `crates/arco-catalog/src/tier1_events.rs`
- Modify: `crates/arco-catalog/src/tier1_state.rs`
- Modify: `crates/arco-catalog/src/tier1_snapshot.rs`
- Modify: `crates/arco-uc/src/routes/tables.rs`
- Modify: `crates/arco-api/src/routes/catalogs.rs`
- Create: `crates/arco-catalog/tests/managed_delta_table_contract.rs`
- Modify: `crates/arco-catalog/tests/schema_contracts.rs`

**Step 1: Write creation contract tests**

Add tests requiring managed Delta tables to persist:

- stable table ID
- table type
- storage location
- effective format `delta`
- managed Delta mode flag
- Delta protocol minimums
- accepted table feature set
- canonical table properties

Add rejection tests for:

- missing or conflicting table location
- unsupported format/protocol combinations
- attempting to create a managed table under an external location without a binding
- attempting to set managed-only properties on an external table

**Step 2: Add typed managed Delta metadata**

Add typed metadata to the catalog state instead of relying only on free-form properties. Preserve compatibility properties as derived output where needed.

**Step 3: Update API and UC table creation**

Make Arco-native and UC-compatible table creation both call the same catalog writer validation. UC should remain an adapter over Arco's table contract.

**Step 4: Run focused tests**

Run: `cargo test -p arco-catalog --test managed_delta_table_contract -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-uc --test preview_crud -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-catalog/src/writer.rs crates/arco-catalog/src/tier1_events.rs crates/arco-catalog/src/tier1_state.rs crates/arco-catalog/src/tier1_snapshot.rs crates/arco-uc/src/routes/tables.rs crates/arco-api/src/routes/catalogs.rs crates/arco-catalog/tests/managed_delta_table_contract.rs crates/arco-catalog/tests/schema_contracts.rs
git commit -m "feat: enforce managed delta table creation contract"
```

### Task 6: Add Staging Table And Initial Commit Finalization

**Files:**
- Modify: `crates/arco-uc/src/routes/tables.rs`
- Modify: `crates/arco-uc/src/openapi.rs`
- Modify: `crates/arco-catalog/src/writer.rs`
- Modify: `crates/arco-catalog/src/reader.rs`
- Create: `crates/arco-uc/tests/staging_tables_authoritative.rs`
- Create: `crates/arco-catalog/tests/staging_table_lifecycle.rs`

**Step 1: Write staging lifecycle tests**

Cover:

- staging table creation reserves table identity and storage location
- finalization requires a valid first Delta commit
- abandoned staging tables are visible for cleanup/retry
- finalized tables become normal managed Delta tables
- table names cannot resolve to partially staged tables unless explicitly requested

**Step 2: Implement staging state**

Persist staging objects through the catalog ledger and publish them in snapshots with explicit lifecycle state.

**Step 3: Implement finalization path**

Finalize by validating the first commit, writing coordinator state, and promoting the table to visible managed Delta state through the normal publication boundary.

**Step 4: Run focused tests**

Run: `cargo test -p arco-uc --test staging_tables_authoritative -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-catalog --test staging_table_lifecycle -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-uc/src/routes/tables.rs crates/arco-uc/src/openapi.rs crates/arco-catalog/src/writer.rs crates/arco-catalog/src/reader.rs crates/arco-uc/tests/staging_tables_authoritative.rs crates/arco-catalog/tests/staging_table_lifecycle.rs
git commit -m "feat: add authoritative managed delta staging lifecycle"
```

### Task 7: Integrate Grants, Credentials, And Audit Into Managed Delta Writes

**Files:**
- Modify: `crates/arco-api/src/routes/delta.rs`
- Modify: `crates/arco-uc/src/routes/delta_commits.rs`
- Modify: `crates/arco-uc/src/routes/credentials.rs`
- Modify: `crates/arco-uc/src/routes/permissions.rs`
- Modify: `crates/arco-uc/src/audit.rs`
- Create: `crates/arco-uc/tests/managed_delta_write_authorization.rs`
- Create: `crates/arco-uc/tests/credentials_authoritative.rs`

**Step 1: Write authorization tests**

Cover:

- write commit is denied without table write privilege
- table credential mint is denied without read/write privilege for requested operation
- path credential mint is denied when the path is outside a governed external location or managed root
- successful allow and deny decisions emit audit events

**Step 2: Call compiled authorization from Delta routes**

Check caller principal, securable object, operation, and inherited grants before staging or committing.

**Step 3: Replace placeholder temporary credentials**

Issue scoped temporary credentials only after resolving table/location and evaluating grants. Responses must contain expiry, operation, storage scope, and provider-specific credential payloads.

**Step 4: Run focused tests**

Run: `cargo test -p arco-uc --test managed_delta_write_authorization -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-uc --test credentials_authoritative -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-api/src/routes/delta.rs crates/arco-uc/src/routes/delta_commits.rs crates/arco-uc/src/routes/credentials.rs crates/arco-uc/src/routes/permissions.rs crates/arco-uc/src/audit.rs crates/arco-uc/tests/managed_delta_write_authorization.rs crates/arco-uc/tests/credentials_authoritative.rs
git commit -m "feat: authorize managed delta writes and credentials"
```

### Task 8: Publish Managed Delta Operational Projections

**Files:**
- Modify: `crates/arco-delta/src/coordinator.rs`
- Modify: `crates/arco-catalog/src/tier1_snapshot.rs`
- Modify: `crates/arco-api/src/system_tables.rs`
- Modify: `docs/guide/src/reference/system-catalog.md`
- Create: `crates/arco-api/tests/system_delta_tables_api.rs`
- Create: `crates/arco-catalog/tests/golden_schemas/delta_tables.schema.json`
- Create: `crates/arco-catalog/tests/golden_schemas/delta_commits.schema.json`

**Step 1: Write system-table tests**

Require queryable projections such as:

- `system.delta.tables`
- `system.delta.commits`
- `system.delta.staged_commits`
- `system.delta.reconciliation_issues`

Keep these read-only and tenant/workspace scoped.

**Step 2: Materialize projections**

Project coordinator/table state into Parquet through the existing snapshot publication model. Do not expose raw coordinator JSON.

**Step 3: Register system tables**

Extend the explicit system-table allowlist so the new Delta projections are queryable through `/api/v1/query`.

**Step 4: Run focused tests**

Run: `cargo test -p arco-api --test system_delta_tables_api -- --nocapture`

Expected: PASS.

Run: `cargo test -p arco-catalog --test schema_contracts -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-delta/src/coordinator.rs crates/arco-catalog/src/tier1_snapshot.rs crates/arco-api/src/system_tables.rs docs/guide/src/reference/system-catalog.md crates/arco-api/tests/system_delta_tables_api.rs crates/arco-catalog/tests/golden_schemas/delta_tables.schema.json crates/arco-catalog/tests/golden_schemas/delta_commits.schema.json
git commit -m "feat: expose managed delta operational system tables"
```

### Task 9: Add Repair And Reconciliation Tooling

**Files:**
- Create: `crates/arco-delta/src/reconciler.rs`
- Modify: `crates/arco-delta/src/lib.rs`
- Create: `crates/arco-delta/tests/reconciliation.rs`
- Create: `docs/runbooks/managed-delta-reconciliation.md`

**Step 1: Write reconciliation tests**

Cover:

- coordinator says committed but `_delta_log` file is missing
- `_delta_log` file exists but coordinator state is behind
- staged payload expired before commit
- idempotency marker exists but final coordinator state is missing

**Step 2: Implement explicit reconciliation**

Use object-store listing only inside this explicit maintenance path. Produce repair findings and require a separate action for destructive or state-changing repair.

**Step 3: Document operator runbook**

Write a runbook that explains diagnostics, safe repair order, and what evidence to capture before repair.

**Step 4: Run focused tests**

Run: `cargo test -p arco-delta --test reconciliation -- --nocapture`

Expected: PASS.

**Step 5: Commit**

```bash
git add crates/arco-delta/src/reconciler.rs crates/arco-delta/src/lib.rs crates/arco-delta/tests/reconciliation.rs docs/runbooks/managed-delta-reconciliation.md
git commit -m "feat: add managed delta reconciliation tooling"
```

### Task 10: Final Verification And Closeout

**Files:**
- Modify: `docs/guide/src/reference/control-plane-scope.md`
- Modify: `docs/guide/src/reference/evidence-policy.md`
- Create: `docs/reports/2026-05-managed-delta-correctness-evidence.md`

**Step 1: Update implementation status**

Move only the verified portions of managed Delta from `Partial` or `Planned` to `Implemented`. Keep grants, credentials, or projections as `Partial` unless their tests and docs landed.

**Step 2: Write evidence report**

Record test commands, exact outcomes, remaining known gaps, and the boundary between normal commit path and reconciliation path.

**Step 3: Run full verification**

Run every command in the verification matrix.

Expected: all commands pass.

**Step 4: Commit**

```bash
git add docs/guide/src/reference/control-plane-scope.md docs/guide/src/reference/evidence-policy.md docs/reports/2026-05-managed-delta-correctness-evidence.md
git commit -m "docs: record managed delta correctness evidence"
```
