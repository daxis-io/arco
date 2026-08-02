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
| State-store seam + current adapter (Phase 1A) | `Partial` | None; current Tier-1 ledger append -> sync compaction remains sole authority | `ArcoStateReader`/`ArcoStateStore`/`ArcoStateTxn`/`StateToken` seam types exist and are CI-tested (`crates/arco-catalog/src/state_store.rs`); the `CurrentStateStore` adapter intentionally exposes capability discovery only and delegates no production reads or writes |
| Deterministic state model (Phase 3A) | `Partial` | None | `ModelStateStore` reference backend with point/range/predicate preconditions and replay determinism proven in CI (`crates/arco-catalog/src/state_store/model.rs`); reference model only, zero production callers |
| Object-store control-store MVP (Phase 3B) | `Partial` | None | `ControlMvpStateStore` txlog + manifest + pointer-CAS prototype (`crates/arco-catalog/src/state_store/control_mvp.rs`); prototype-approved only, not accepted production architecture; replay is unbounded from genesis (#334) and publish has no writer-epoch fencing; zero production callers |
| Prototype promotion gate (Phase 3C) | `Partial` | None | Advisory evaluator exists and is CI-tested (`crates/arco-catalog/src/state_store/promotion_gate.rs`) but has never run with real measurements; no control-MVP benchmark or recorded evidence packet exists; the control store is NOT promoted |
| Shadow replay importer (Phase 4A) | `Partial` | None | `state_store/shadow_replay.rs` imports Tier-1 catalog state into an isolated `catalog-shadow` domain; covers 3 of 9 mandated comparison domains; importer has zero non-test callers, so no deployed shadow store is ever populated |
| Internal comparison reads (Phase 4B) | `Partial` | None | `state_store/comparison_reads.rs` behind `ARCO_CATALOG_SHADOW_COMPARE_READS`; one internal read path (catalog inventory descriptor) compares current vs shadow, diagnostics only; inert while the 4A importer never runs |
| Projection-outbox-acks writable domain (Phase 5) | `Partial` | None | First control-store writable domain (`state_store/projection_outbox_acks.rs`), idempotent acks + token-pinned reads at unit level; crate-private with zero non-test callers and not wired to the real arco-flow outbox |
| Storage-governance metadata domains (Phase 6) | `Partial` | None | `path_governance_metadata.rs`, `external_location_metadata.rs`, `workspace_binding_metadata.rs` with ancestor/descendant predicate model and deny-closed readiness helpers, CI-tested; no authority moved; credential vending does not read these domains; the revocation-freshness budget required before any grants migration is undefined |
| Workspace snapshots + export manifest (Phase 7A) | `Partial` | None | `workspace_snapshot.rs` snapshot/export contracts, GC ProtectionSet, and `system.catalog.snapshots` exact-schema projection, all CI-tested; pins can only reference control-MVP checkpoints, so no production authority is pinnable yet; no producer writes `snapshots.parquet` |
| Workspace snapshot service (Phase 7B) | `Partial` | None | `workspace_snapshot_service.rs` Create/Get/Export with DistributedLock + retention-mutation-epoch coordination and read-only restore preflight, CI-tested; constructed by zero production code (no route or binary) |
| Roll-forward restore (Phase 7C) | `Partial` | None | `workspace_restore.rs` deterministic plan/inspect/apply with crash-resume and REPAIR_REQUIRED journal tests; only the control-MVP `StateRestoreParticipant` exists; catalog/orchestration domains have no typed restore operation |
| Durable transaction handles (Phase 7D) | `Partial` | None | Full OPEN..REPAIR_REQUIRED lifecycle state machine with 94 unit tests (`arco-api/src/control_plane_transactions/handles.rs`); deliberately transport-less (`#![cfg_attr(not(test), allow(dead_code))]`), unreachable in production builds; the legacy-handle identity guard IS live in the production `claim_idempotency` path |

## Current Thesis, Narrowly Stated

The repo proves this statement today:

> Arco is a file-native catalog and metastore for open lakehouse table formats. It has an immutable-commit control plane for catalog DDL, lineage/search materialization, orchestration transactions, and Delta coordinated commit state, with fenced head publication as the visibility boundary.

It also contains an initial native metastore replay/projection kernel that
proves stable-ID folding, projection allowlisting, schema watermarking, and
redaction for the first generic metastore projection.

It additionally contains the Phase 1A–7D state-store program surfaces
(seam traits, deterministic model, object-store control-store MVP, promotion
gate, shadow replay, comparison reads, projection-outbox-acks, storage-
governance metadata, workspace snapshots/export/restore, durable transaction
handles). These are landed with CI-run test suites but are deliberately
non-authoritative: crate-private, with zero production callers, and the
control-store prototype has not passed its Phase 3C promotion gate. Do not
describe any of them as production control-plane authority.

The repo does not yet prove this broader statement:

> Every governance and metadata object in the catalog is already managed through the same authoritative immutable control-plane ledger.

## Related References

- `docs/plans/2026-06-27-arco-unified-execution-roadmap.md`
- `docs/reports/2026-07-30-design-program-progress-audit.md`
- `docs/reports/2026-04-20-catalog-control-plane-framing-audit.md`
- `docs/adr/adr-018-tier1-write-path.md`
- `docs/adr/adr-032-immutable-manifest-pointers.md`
- `docs/adr/adr-034-fenced-head-published-control-plane-transactions.md`
