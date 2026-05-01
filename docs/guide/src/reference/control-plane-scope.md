# Control-Plane Scope

This page is the repo-local scorecard for Arco's control-plane scope.

Use it to answer a narrow question:

> What is authoritative in the current repo, what is only partial, and what is still planned?

Status meanings:

- `Implemented`: authoritative path exists in code, has tests, and is exercised by CI
- `Partial`: some real implementation exists, but the scope is narrower than the intended framing or parallel non-authoritative paths still exist
- `Planned`: documented intent, parity scaffolding, or placeholder behavior only

Implementation claims on this page should satisfy the evidence policy in `docs/guide/src/reference/evidence-policy.md`.

## Scorecard

| Area | Status | Current authoritative path | Notes |
|---|---|---|---|
| Catalog DDL: catalogs, schemas, tables, columns | `Implemented` | `CatalogWriter` -> ledger append -> sync compaction -> immutable manifest snapshot -> pointer CAS | See `crates/arco-catalog/src/writer.rs` and `crates/arco-catalog/src/tier1_compactor.rs` |
| Lineage domain | `Implemented` | Lineage ledger/events -> lineage snapshot -> pointer CAS | Separate manifest/lock domain from catalog DDL |
| Search index | `Implemented` | Derived from current catalog state, then published via immutable snapshot + pointer CAS | Serving/index state is derived, not authoritative |
| Orchestration transactions | `Implemented` | Orchestration events -> compaction -> immutable manifest snapshot -> pointer CAS | See `proto/arco/controlplane/v1/transactions.proto` |
| Root transactions for pinned `catalog` + `orchestration` reads | `Implemented` | Root tx record + immutable super-manifest | Cross-domain pinning exists, but it is scoped |
| Delta commit coordination | `Implemented` | Coordinator state + CAS/idempotency flow | Table-scoped control-plane subsystem |
| UC native parity for catalogs/schemas/tables | `Implemented` | UC catalog/schema/table routes use `CatalogWriter`/`CatalogReader` over the authoritative catalog ledger and manifest-published snapshots | Catalog/schema PATCH now authoritatively persists `comment`, `new_name`, `properties`, and `storage_root`; table create/get/list round-trips authoritative `table_type` and `properties`. Remaining preview/scaffolded UC surfaces are outside catalog/schema/table CRUD |
| Broader "catalog as control-plane ledger" framing | `Partial` | Real today for catalog DDL, lineage/search publication, orchestration transactions, and delta coordination | Broader governance domains are not yet on the same authoritative path |
| Grants / RBAC | `Planned` | None in authoritative protobuf/API/catalog writer path | ADR intent exists, but no implemented authoritative path |
| Permissions/authz state | `Planned` | None | Current UC permissions route is parity scaffolding and not backed by an authoritative grants store |
| Credentials / storage bindings / external locations | `Planned` | None | Current UC credential routes are stubbed or placeholder behavior, not catalog-ledger state |
| Policies, masking, classifications, governance rules | `Planned` | None | Not yet modeled in the authoritative control-plane transaction surface |
| Ownership / tags as authoritative control-plane state | `Planned` | Data types exist, but not authoritative transaction-managed state | Do not describe these as implemented governance control-plane objects |

## Current Thesis, Narrowly Stated

The repo proves this statement today:

> Arco has an immutable-commit control plane for catalog DDL, lineage/search materialization, orchestration transactions, and Delta coordinated commit state, with fenced head publication as the visibility boundary.

The repo does not yet prove this broader statement:

> Every governance and metadata object in the catalog is already managed through the same authoritative immutable control-plane ledger.

## Related References

- `docs/reports/2026-04-20-catalog-control-plane-framing-audit.md`
- `docs/adr/adr-018-tier1-write-path.md`
- `docs/adr/adr-032-immutable-manifest-pointers.md`
- `docs/adr/adr-034-fenced-head-published-control-plane-transactions.md`
