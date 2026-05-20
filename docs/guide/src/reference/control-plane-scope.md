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
| Metastore/governance protobuf surface | `Partial` | `arco.catalog.v1` metastore messages plus root transaction mutation envelope | Wire contract exists in `proto/arco/catalog/v1/metastore.proto`; root metastore commits currently return `NOT_IMPLEMENTED`; changes must pass `cargo xtask proto-breaking-check` unless they are part of an explicit alpha/beta hard-cut window |
| Metastore replay/projection kernel | `Partial` | `crates/arco-catalog/src/metastore/` folds initial native metastore events and builds allowlisted `metastore_objects.parquet` rows | This is a narrow kernel with schema watermarking and redaction tests; object-family projections, writer integration, route enforcement, and system-table exposure remain pending |
| Table-format catalog contract | `Implemented` | `TableFormat` accepts Delta Lake, Iceberg, and plain Parquet; new table registration defaults to Delta | Legacy rows without persisted format metadata still read as Parquet; Iceberg and Parquet support do not imply full governance parity yet |
| Delta commit coordination | `Implemented` | Coordinator state + CAS/idempotency flow | Table-scoped control-plane subsystem |
| UC native parity for catalogs/schemas/tables | `Implemented` | UC catalog/schema/table routes use `CatalogWriter`/`CatalogReader` over the authoritative catalog ledger and manifest-published snapshots | Catalog/schema PATCH now authoritatively persists `comment`, `new_name`, `properties`, and `storage_root`; table create/get/list round-trips authoritative `table_type` and `properties`. Remaining preview/scaffolded UC surfaces are outside catalog/schema/table CRUD |
| Broader "catalog as control-plane ledger" framing | `Partial` | Real today for catalog DDL, lineage/search publication, orchestration transactions, delta coordination, and the initial metastore replay/projection kernel | Broader governance domains are not yet production-backed through writer APIs, enforcement, or system tables |
| Grants / RBAC | `Planned` | Contract and initial replay/projection kernel types exist; no authoritative catalog writer or API enforcement path | ADR intent exists, but UC permissions are not backed by a compiled grants store yet |
| Permissions/authz state | `Planned` | None | Current UC permissions route is parity scaffolding and not backed by an authoritative grants store |
| Storage credentials | `Planned` | Contract and initial replay/projection kernel types exist; no authoritative credential writer, provider integration, or API enforcement path | Current UC credential routes are stubbed or placeholder behavior, not catalog-ledger state |
| Service credentials | `Planned` | None | Roadmap object family; no authoritative contract or route behavior |
| External service connections | `Planned` | None | Roadmap object family; no authoritative contract or route behavior |
| External locations | `Planned` | Metastore proto contracts exist; no authoritative catalog writer/projection or API enforcement path | Current UC external-location route behavior is not catalog-ledger-backed |
| Managed storage roots | `Planned` | None | Required for governed path ownership, but not yet authoritative state |
| Views | `Planned` | None | Views are a planned securable object family; query expansion/execution is out of current scope |
| Volumes | `Planned` | Metastore proto contracts exist; no authoritative catalog writer/projection or API enforcement path | UC inventory has route shapes, but Arco-native state is not implemented |
| Functions | `Planned` | Metastore proto contracts exist; no authoritative catalog writer/projection or API enforcement path | Metadata object family only; execution is out of scope |
| Models / model versions | `Planned` | Metastore proto contracts exist; no authoritative catalog writer/projection or API enforcement path | Model artifact ownership and credential vending are planned |
| Shares / providers / recipients | `Planned` | None | Roadmap compatibility surface; no current authoritative state |
| Policies, masking, classifications, governance rules | `Planned` | `GovernanceAttachment` proto contract exists; no authoritative catalog writer/projection or policy enforcement path | Not yet modeled as authoritative runtime state |
| Glossary terms / data products / business domains | `Planned` | None | Product taxonomy and metadata domains are design-level only |
| Ownership / tags as authoritative control-plane state | `Planned` | Data types and metastore attachment contracts exist, but not authoritative transaction-managed state | Do not describe these as implemented governance control-plane objects |
| Temporary credential vending | `Planned` | None | Current temporary credential routes are placeholder behavior and not backed by compiled authorization decisions |
| Access audit | `Planned` | Tracing/audit hooks exist, but no authoritative catalog access-audit projection | System tables for access audit remain deferred |
| Storage/system tables beyond initial catalog lineage orchestration surface | `Planned` | None | `system.access.*`, `system.storage.*`, and extended catalog object-family tables are not registered until projections exist |

## Current Thesis, Narrowly Stated

The repo proves this statement today:

> Arco is a file-native catalog and metastore for open lakehouse table formats. It has an immutable-commit control plane for catalog DDL, lineage/search materialization, orchestration transactions, and Delta coordinated commit state, with fenced head publication as the visibility boundary.

It also contains an initial native metastore replay/projection kernel that
proves stable-ID folding, projection allowlisting, schema watermarking, and
redaction for the first generic metastore projection.

The repo does not yet prove this broader statement:

> Every governance and metadata object in the catalog is already managed through the same authoritative immutable control-plane ledger.

## Related References

- `docs/reports/2026-04-20-catalog-control-plane-framing-audit.md`
- `docs/adr/adr-018-tier1-write-path.md`
- `docs/adr/adr-032-immutable-manifest-pointers.md`
- `docs/adr/adr-034-fenced-head-published-control-plane-transactions.md`
