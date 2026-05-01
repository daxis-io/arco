# Catalog Control-Plane Framing Audit

Date: April 20, 2026

Update: April 22, 2026

UC catalog/schema/table CRUD was routed to the authoritative catalog path after
this audit. Treat the UC preview CRUD findings below as historical for that
surface; the remaining preview/scaffolded findings still apply to endpoints
outside catalog/schema/table CRUD.

## Scope

This audit answers one question:

> Which parts of the Arco framing "the catalog is the control-plane ledger over immutable commits" have executable proof in this repo, and which parts are still intent?

The framing under review is the stronger Arco-native version:

- the catalog is not only a lookup table
- immutable commits are the source of truth
- visibility is controlled by a small mutable head/pointer CAS
- serving state is derived, not authoritative
- the control plane spans more than per-table metadata

This audit is repo-grounded. It distinguishes between:

- `Proven`: implemented in code, backed by automated tests, and tied to CI/workflow evidence
- `Partial`: meaningful implementation exists, but the scope is narrower than the framing or parallel non-authoritative paths still exist
- `Intent`: ADRs/docs/spec parity/stubs exist, but the authoritative runtime path is not implemented in this repo

## Evidence Standard

Per `docs/guide/src/reference/evidence-policy.md`, an "implemented" claim should have:

1. a concrete code reference
2. at least one test reference
3. the CI job/command that exercises that proof

This audit uses that standard. Pure ADR language, OpenAPI parity, or route scaffolding does not count as executable proof by itself.

## Executive Verdict

Arco has strong executable proof for the publication protocol:

- immutable manifest snapshots
- fenced pointer CAS as the commit point
- pointer-first readers
- no-list correctness paths for catalog/orchestration readers
- pinned root transactions for catalog plus orchestration
- append-only event ingestion with compactor-owned materialization

Arco does **not** yet have equally strong executable proof for the full governance/control-plane scope described in the framing.

The repo proves:

- `catalog DDL + lineage + search + orchestration` as immutable, head-published control-plane state

The repo does **not** yet prove:

- grants / RBAC as authoritative Tier-1 state
- permissions/authz as ledger-backed current policy state
- policy attach/detach / masking / classification as immutable control-plane commits
- credential bindings / storage credentials / external locations as authoritative catalog-ledger state

The strongest remaining drift from the framing is that Unity Catalog-facing surfaces outside catalog/schema/table CRUD still run through parity scaffolding or domain-specific preview behavior rather than the authoritative immutable-manifest control plane.

## Claim Matrix

| Framing claim | Status | Evidence | Notes |
|---|---|---|---|
| Immutable metadata snapshots are the source of truth for visible control-plane state | Proven | `docs/adr/adr-032-immutable-manifest-pointers.md`, `docs/adr/adr-034-fenced-head-published-control-plane-transactions.md`, `crates/arco-catalog/src/tier1_compactor.rs`, `crates/arco-core/tests/publish_protocol_contract.rs` | Catalog, lineage, search, and orchestration all use immutable snapshot plus pointer publication |
| The commit point is the fenced CAS move of the current head/pointer, not the immutable write | Proven | `crates/arco-catalog/src/tier1_compactor.rs`, `crates/arco-core/tests/publish_protocol_contract.rs`, `crates/arco-catalog/tests/protocol_invariants.rs`, `docs/reports/adr-034-protocol-invariant-evidence-matrix.md` | This is one of the best-proven parts of the architecture |
| Readers are pointer-first / manifest-driven and do not rely on listing or ledger scans for correctness | Proven | `crates/arco-catalog/tests/protocol_invariants.rs`, `crates/arco-flow/tests/orchestration_protocol_invariants.rs`, `docs/reports/adr-034-protocol-invariant-evidence-matrix.md` | Includes ordinary reads and root-token reads |
| API/control-plane services append events and trigger compaction; compactors are sole writers for materialized Parquet state | Proven | `docs/adr/adr-018-tier1-write-path.md`, `crates/arco-catalog/src/writer.rs`, `docs/adr/adr-032-engine-boundaries.md`, `crates/arco-core/tests/ui.rs`, `.github/workflows/ci.yml` | This matches the intended split-service boundary |
| The catalog is a real control-plane surface, not just per-table state | Partial | `proto/arco/catalog/v1/catalog.proto`, `proto/arco/controlplane/v1/transactions.proto`, `crates/arco-api/src/control_plane_transactions.rs`, `crates/arco-catalog/src/writer.rs` | Real for catalogs, schemas, tables, lineage, search; not real for broader governance objects |
| There is a catalog-level snapshot boundary rather than only per-table snapshots | Proven | `crates/arco-catalog/src/tier1_compactor.rs`, `crates/arco-catalog/src/tier1_snapshot.rs`, `crates/arco-catalog/src/manifest.rs` | Catalog snapshots materialize catalogs, namespaces, tables, and columns together |
| Cross-domain coherent reads can be pinned to an immutable multi-domain cut | Partial | `proto/arco/controlplane/v1/transactions.proto`, `crates/arco-api/src/control_plane_transactions.rs`, `crates/arco-api/tests/root_transaction_protocol.rs` | Real for `catalog` plus `orchestration`; not generalized across all control-plane domains |
| Serving/index layers are derived state rather than source of truth | Partial | `crates/arco-catalog/src/tier1_compactor.rs` search compaction, `crates/arco-catalog/src/state.rs`, `crates/arco-catalog/src/tier1_snapshot.rs` | Clearly true for search; not yet equally true for permissions/authz/credentials |
| The governance surface includes grants/RBAC in the same authoritative immutable commit model | Intent | `docs/adr/adr-030-delta-uc-metastore.md` only | No authoritative proto/catalog-writer/API/test path found for grants |
| Permissions/authz are served from current authoritative state with mutation support | Intent | `crates/arco-uc/src/routes/permissions.rs`, `crates/arco-uc/tests/discovery_endpoints.rs` | `GET` returns empty assignments; `PATCH` is explicitly unsupported |
| Credentials/storage bindings are authoritative catalog-ledger state | Intent | `crates/arco-uc/src/routes/credentials.rs`, `crates/arco-uc/tests/discovery_endpoints.rs` | Mostly stubbed or placeholder; not on the catalog transaction path |
| UC catalog/schema/table CRUD operations ride the same authoritative immutable-manifest control plane | Proven | `crates/arco-uc/src/routes/catalogs.rs`, `crates/arco-uc/src/routes/schemas.rs`, `crates/arco-uc/src/routes/tables.rs`, `crates/arco-uc/tests/preview_crud.rs`, `crates/arco-catalog/src/writer.rs` | UC CRUD now persists through the catalog ledger and manifest-published snapshots; remaining UC scaffolding is outside catalog/schema/table CRUD |
| Delta coordinated commit control plane exists as immutable/idempotent control-plane state | Proven | `crates/arco-delta/src/coordinator.rs`, `crates/arco-uc/src/routes/delta_commits.rs`, `crates/arco-uc/tests/delta_commit_coordinator_semantics.rs` | This is a strong proof for a table-scoped control-plane subsystem |
| Retention, GC, and orphan cleanup exist as part of the architecture | Proven | `crates/arco-catalog/src/gc/`, `crates/arco-catalog/src/reconciler.rs`, `crates/arco-flow/src/orchestration/compactor/reconciler.rs`, `docs/runbooks/gc-failure.md` | This part is operationally real, not just conceptual |

## Proven Areas

### 1. Immutable snapshot + pointer CAS publication is real

This is the most mature and best-proven part of the framing.

Primary implementation and proof:

- `docs/adr/adr-032-immutable-manifest-pointers.md`
- `docs/adr/adr-034-fenced-head-published-control-plane-transactions.md`
- `crates/arco-catalog/src/tier1_compactor.rs`
- `crates/arco-core/tests/publish_protocol_contract.rs`
- `crates/arco-catalog/tests/protocol_invariants.rs`
- `crates/arco-flow/tests/orchestration_protocol_invariants.rs`
- `docs/reports/adr-034-protocol-invariant-evidence-matrix.md`

What is actually proven:

- immutable manifests are written with `DoesNotExist`
- visibility advances only through pointer/head publication
- stale fencing holders are rejected
- CAS races preserve old-or-winner visible state
- readers follow the pointer/manifests rather than reconstructing from bucket listing or ledger scans

This is not architectural aspiration. It is implemented behavior with tests and CI coverage.

### 2. Append-only event ingestion plus compactor-owned materialization is real

Primary implementation and proof:

- `docs/adr/adr-018-tier1-write-path.md`
- `docs/adr/adr-032-engine-boundaries.md`
- `crates/arco-catalog/src/writer.rs`
- `crates/arco-core/tests/ui.rs`
- `.github/workflows/ci.yml`

`CatalogWriter` methods such as `create_catalog`, `create_schema`, `register_table`, `update_table`, `drop_table`, and `rename_table` all follow the same basic pattern:

1. acquire domain lock
2. load current manifest-selected state
3. append an event to the ledger
4. invoke synchronous compaction
5. rely on compactor publication for visibility

This is consistent with the framing that the authoritative transition is durable immutable write plus head advance, not direct row mutation.

### 3. Catalog snapshots are catalog-level, not only per-table

Primary implementation and proof:

- `crates/arco-catalog/src/tier1_compactor.rs`
- `crates/arco-catalog/src/tier1_snapshot.rs`
- `crates/arco-catalog/src/manifest.rs`
- `crates/arco-catalog/tests/schema_contracts.rs`

The authoritative catalog snapshot materializes:

- catalogs
- namespaces/schemas
- tables
- columns

That means Arco is not modeling authoritative state as isolated per-table snapshots only. On this point, the implementation matches the framing.

### 4. Root transactions exist for pinned cross-domain reads

Primary implementation and proof:

- `proto/arco/controlplane/v1/transactions.proto`
- `crates/arco-api/src/control_plane_transactions.rs`
- `crates/arco-core/tests/control_plane_transaction_paths_contracts.rs`
- `crates/arco-api/tests/control_plane_transactions_api.rs`
- `crates/arco-api/tests/root_transaction_protocol.rs`

This is real, but deliberately scoped:

- ordinary readers remain domain-local
- root tokens pin immutable domain heads
- the pinned root cut currently covers `catalog` plus `orchestration`

This proves Arco can produce an immutable cross-domain read token. It does not yet prove a generalized catalog-wide governance snapshot across every metadata domain.

### 5. Search is a derived index, not an independent source of truth

Primary implementation and proof:

- `crates/arco-catalog/src/tier1_compactor.rs`
- `crates/arco-catalog/src/state.rs`
- `crates/arco-catalog/src/tier1_snapshot.rs`
- `docs/adr/adr-003-manifest-domains.md`

`sync_compact_search` rebuilds search state from current catalog state and then publishes it with the same immutable snapshot plus pointer pattern. That is exactly the "derived serving/index layer" part of the framing, at least for search.

### 6. Delta commit coordination is implemented as control-plane state

Primary implementation and proof:

- `crates/arco-delta/src/coordinator.rs`
- `crates/arco-uc/src/routes/delta_commits.rs`
- `crates/arco-uc/tests/delta_commit_coordinator_semantics.rs`

This is notable because it proves Arco can own a table-adjacent control-plane coordinator without falling back to a conventional mutable OLTP metastore.

The delta coordinator is not just API parity. It has:

- idempotency
- inflight reservation state
- CAS-based finalization
- retry/recovery logic
- executable tests around conflicts and replay

## Partial Areas

### 1. "Catalog as control-plane ledger" is real only for a subset of control-plane scope

Proven scope:

- catalogs
- schemas
- tables
- columns
- lineage domain
- search domain
- orchestration transactions

Evidence:

- `proto/arco/catalog/v1/catalog.proto`
- `proto/arco/controlplane/v1/transactions.proto`
- `crates/arco-api/src/control_plane_transactions.rs`
- `crates/arco-catalog/src/writer.rs`

Missing from the same authoritative surface:

- grants / privilege mutations
- policy attach/detach
- masking/classification
- storage credentials / external locations
- credential bindings

So the repo proves a strong metadata ledger, but not yet the full governance ledger described in the framing.

### 2. Cross-domain coherence is implemented, but only for selected domains

The framing suggests a coherent catalog/control-plane snapshot. Arco currently has:

- domain-local heads for catalog, lineage, search, and orchestration
- pinned root transactions for catalog plus orchestration

Evidence:

- `docs/adr/adr-003-manifest-domains.md`
- `docs/adr/adr-034-fenced-head-published-control-plane-transactions.md`
- `proto/arco/controlplane/v1/transactions.proto`

This is good progress, but it is not the same as one generalized commit graph covering all governance domains.

### 3. UC compatibility exists, but not uniformly on the authoritative path

There are two very different states in the repo:

1. Native Arco catalog APIs and transaction APIs:
   - use `CatalogWriter`
   - append events
   - sync compact
   - publish via immutable manifest pointer

2. UC facade preview CRUD:
   - writes JSON objects under `unity-catalog-preview/...`
   - uses prefix `list()` for pagination and discovery
   - does not use the authoritative catalog ledger path

Primary evidence:

- authoritative path:
  - `crates/arco-api/src/routes/catalogs.rs`
  - `crates/arco-api/src/routes/tables.rs`
  - `crates/arco-api/src/control_plane_transactions.rs`
- preview path:
  - `crates/arco-uc/src/routes/catalogs.rs`
  - `crates/arco-uc/src/routes/schemas.rs`
  - `crates/arco-uc/src/routes/tables.rs`
  - `crates/arco-uc/src/routes/preview.rs`
  - `crates/arco-uc/tests/preview_crud.rs`

This is the clearest architecture drift in the repo. The UC preview surface proves API experimentation and parity work, not convergence on the authoritative ledger model.

## Intent / Spec / Stub Areas

### 1. Grants / RBAC as authoritative Tier-1 state

Intent evidence:

- `docs/adr/adr-030-delta-uc-metastore.md`
- `docs/adr/adr-031-unity-catalog-api-facade.md`

What is missing:

- no grant operations in `proto/arco/catalog/v1/catalog.proto`
- no grant mutations in `proto/arco/controlplane/v1/transactions.proto`
- no authoritative grant writer path in `crates/arco-catalog/src/writer.rs`
- no grant-focused protocol or API tests in the main control-plane suites

Conclusion:

`grants.parquet` and compiled RBAC are architectural intent in this repo, not executable proof.

### 2. Permissions/authz as current authoritative policy state

Primary evidence:

- `crates/arco-uc/src/routes/permissions.rs`
- `crates/arco-uc/tests/discovery_endpoints.rs`

Actual behavior:

- `GET /permissions/{...}` returns `"privilege_assignments": []`
- `PATCH /permissions/{...}` is explicitly unsupported
- tests validate the empty/success behavior, not real policy evaluation

Conclusion:

The repo proves the presence of a route and a parity-shaped response, not a real authorization control plane.

### 3. Credential bindings and storage credentials as authoritative control-plane state

Primary evidence:

- `crates/arco-uc/src/routes/credentials.rs`
- `crates/arco-uc/tests/discovery_endpoints.rs`
- `crates/arco-api/src/config.rs`

Actual behavior:

- model-version and volume credential routes are unsupported
- temporary table credentials are scaffolded but return `NotImplemented`
- temporary path credentials validate the request and return a placeholder object with `"credentials": []`

Conclusion:

Credential and storage-binding governance is not yet implemented as authoritative immutable control-plane state in this repo.

### 4. Policy attach/detach, masking, classification, and governance rules

Intent evidence exists in the framing and surrounding docs, but I did not find an authoritative implementation surface for:

- policy objects
- policy attachment or detachment
- masking rules
- data classifications
- governance rules

I also did not find them in:

- `proto/arco/catalog/v1/catalog.proto`
- `proto/arco/controlplane/v1/transactions.proto`
- `crates/arco-catalog/src/writer.rs`
- the main protocol/API transaction test suites

Conclusion:

These are still intent relative to the framing.

### 5. Ownership/tags as authoritative control-plane mutations

There are domain types for owners and tags:

- `crates/arco-catalog/src/asset.rs`

But I did not find those concepts integrated into the authoritative catalog transaction model. In practice they appear as standalone data types or API payload fields, not as proven immutable control-plane commits with a head-published current state.

Conclusion:

Ownership/tags exist as data modeling surface, not yet as proven authoritative control-plane ledger state.

## Important Contradictions and Risks

### 1. UC preview CRUD bypasses the authoritative ledger model

This is the sharpest contradiction against the framing.

`crates/arco-uc/src/routes/preview.rs`:

- writes standalone JSON objects
- uses prefix listing for read paths
- does not use immutable manifest snapshots plus pointer publication

If the long-term thesis is "the catalog is the control-plane ledger," this preview surface is not that ledger.

### 2. Governance scope is wider in docs than in code

The docs imply a future control plane with:

- grants
- permissions
- credentials
- external locations
- broader governance

But the executable transaction surface today is much narrower. This is manageable if named honestly, but it is a real scope gap.

### 3. Root transaction language can overstate current cross-domain guarantees

The repo has real root transactions, but they are not a universal global control-plane snapshot. They currently pin selected domains, not the full governance graph.

### 4. Derived serving-state principle is only fully demonstrated for some domains

Search is clearly derived.
Permissions/authz and credential serving are not yet proven as derived state over the same immutable ledger, because the underlying authoritative governance ledger is not yet implemented.

## Final Classification

### Executable proof exists today

- immutable manifest snapshots plus pointer CAS publication
- fenced commit semantics and stale-writer rejection
- pointer-first manifest-driven reads without correctness-critical listing
- append-only event ingestion plus compactor-owned materialization
- catalog-level DDL snapshotting for catalogs/schemas/tables/columns
- domain split across catalog, lineage, search, and orchestration
- pinned root transactions for catalog plus orchestration
- derived search index publication
- delta commit coordinator semantics
- GC/reconciler/orphan-cleanup operational layer

### Partial proof exists today

- "catalog as control-plane ledger" for metadata domains, but not full governance
- coherent multi-domain pinning, but not across every control-plane domain
- UC compatibility, but split between authoritative native APIs and preview/parity storage paths
- derived serving-state story, but only clearly implemented for some domains

### Still intent today

- grants / compiled RBAC as authoritative immutable catalog state
- permission mutation and enforcement backed by the control-plane ledger
- credential bindings / storage credentials / external locations as authoritative ledger state
- policy attachment, masking, classifications, and governance rules as immutable commits
- ownership/tags as authoritative transaction-managed control-plane state

## Recommended Next Steps

### P0

- Decide whether `crates/arco-uc` preview CRUD is temporary scaffolding or a supported path. If supported, route it through the authoritative catalog transaction/write path instead of `unity-catalog-preview/...` JSON objects.
- Stop describing grants/permissions/credentials/policies as implemented control-plane scope until they have authoritative code paths, tests, and CI evidence.

### P1

- Add an explicit repo-local scorecard for control-plane objects:
  - catalogs
  - schemas
  - tables
  - lineage
  - search
  - orchestration
  - grants
  - permissions
  - credentials
  - policies
  - tags/classifications
- For each object, require:
  - source-of-truth path
  - commit protocol
  - read protocol
  - test proof
  - CI lane

### P1

- Extend the authoritative protobuf and catalog transaction model if grants/policies/credentials are part of the intended Arco control plane.
- Add explicit negative-invariant tests for any newly-authoritative governance domains:
  - no visible success without head advance
  - no correctness-critical list scans
  - stale fences rejected
  - serving state rebuildable from immutable state

### P2

- Replace architecture language like "catalog is the control-plane ledger" with more precise language in user-facing docs until the governance surface catches up:
  - today: "Arco has a proven immutable-commit control plane for catalog DDL, lineage/search materialization, and orchestration transactions"
  - future: "Arco extends that same model to governance objects like grants, policies, and credentials"
