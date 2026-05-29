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
| Metastore/governance protobuf surface | `Partial` | Durable `arco.catalog.v1` metastore messages plus root transaction mutation envelope | Wire contract exists in `proto/arco/catalog/v1/metastore.proto`; additive changes must pass `cargo xtask proto-breaking-check` |
| Metastore replay/projection kernel | `Partial` | `crates/arco-catalog/src/metastore/` folds initial native metastore events and builds allowlisted `metastore_objects.parquet` rows | This is a narrow kernel with schema watermarking and redaction tests; native writer parity and system-table exposure remain pending. Some UC governance adapters now have partial route behavior over scoped metastore and storage-governance state |
| Table-format catalog contract | `Implemented` | `TableFormat` accepts Delta Lake, Iceberg, and plain Parquet; new table registration defaults to Delta | Legacy rows without persisted format metadata still read as Parquet; Iceberg and Parquet support do not imply full governance parity yet |
| Delta commit coordination | `Implemented` | Coordinator state + CAS/idempotency flow | Table-scoped control-plane subsystem |
| UC native parity for catalogs/schemas/tables | `Implemented` | UC catalog/schema/table routes use `CatalogWriter`/`CatalogReader` over the authoritative catalog ledger and manifest-published snapshots; `arco_uc::support` labels these operations `implemented` and exports their OpenAPI support metadata | Catalog/schema PATCH now authoritatively persists `comment`, `new_name`, `properties`, and `storage_root`; table create/get/list round-trips authoritative `table_type` and `properties`. Route-wide compiled-grant enforcement remains separate governance work |
| Broader "catalog as control-plane ledger" framing | `Partial` | Real today for catalog DDL, lineage/search publication, orchestration transactions, delta coordination, the initial metastore replay/projection kernel, and selected route-level UC governance adapters | Broader governance domains are not yet production-backed through native writer APIs, route-wide enforcement, or system tables |
| Grants / RBAC | `Partial` | `GET /permissions/{securable_type}/{full_name}` reads injected compiled assignments; contract and initial replay/projection kernel types also exist | `PATCH /permissions`, writer-backed grant mutation/persistence, grant-option enforcement, grant mutation audit, and native grants store parity remain planned or known-unsupported |
| Permissions/authz state | `Partial` | UC-compatible partial adapters can consume injected compiled permissions and deny closed when required projections are unavailable | This is not yet a manifest-published grants projection or full route-wide authorization enforcement path |
| Storage credentials | `Partial` | Arco-native `/storage-credentials` create/list/get uses scoped metastore ledger state and storage-governance validation | Pinned UC `/credentials` routes are known-unsupported; provider credential material/secret integration, update/delete, service credentials, and system-table exposure remain planned |
| Service credentials | `Planned` | None | Roadmap object family; no authoritative contract or route behavior |
| External service connections | `Planned` | None | Roadmap object family; no authoritative contract or route behavior |
| External locations | `Partial` | `/external-locations` create/list/get uses scoped metastore ledger mutation/replay plus storage-governance path validation | Update/delete, broader binding lifecycle, native governance writer parity, and system-table exposure remain planned |
| Managed storage roots | `Planned` | None | Required for governed path ownership, but not yet authoritative state |
| Views | `Planned` | None | Views are a planned securable object family; query expansion/execution is out of current scope |
| Volumes | `Planned` | Metastore proto contracts exist; no authoritative catalog writer/projection or API enforcement path | UC inventory has route shapes, but Arco-native state is not implemented |
| Functions | `Planned` | Metastore proto contracts exist; no authoritative catalog writer/projection or API enforcement path | Metadata object family only; execution is out of scope |
| Models / model versions | `Planned` | Metastore proto contracts exist; no authoritative catalog writer/projection or API enforcement path | Model artifact ownership and credential vending are planned |
| Shares / providers / recipients | `Planned` | None | Roadmap compatibility surface; no current authoritative state |
| Policies, masking, classifications, governance rules | `Planned` | `GovernanceAttachment` proto contract exists; no authoritative catalog writer/projection or policy enforcement path | Not yet modeled as authoritative runtime state |
| Glossary terms / data products / business domains | `Planned` | None | Product taxonomy and metadata domains are design-level only |
| Ownership / tags as authoritative control-plane state | `Planned` | Data types and metastore attachment contracts exist, but not authoritative transaction-managed state | Do not describe these as implemented governance control-plane objects |
| Temporary credential vending | `Partial` | Table/path credential routes use compiled authorization plus published storage-governance state | Volume/model credentials, provider token material, revocation metadata, and full UC parity remain planned or known-unsupported |
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
