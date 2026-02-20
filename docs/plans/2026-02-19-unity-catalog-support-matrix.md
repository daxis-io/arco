# Unity Catalog Support Matrix (Q2 Interoperability Core)

Last updated: 2026-02-19

## Scope and gating

- Mount prefix: `/api/2.1/unity-catalog`
- Feature gate: `unity_catalog.enabled` (`ARCO_UNITY_CATALOG_ENABLED`)
- Legacy internal mount `/_uc/api/2.1/unity-catalog/*` is retired (legacy module removed from `arco-api`); only the configured mount prefix is exposed.
- Pinned contract source:
  `crates/arco-uc/tests/fixtures/unitycatalog-openapi.yaml`

## Implemented (Q2 core)

| Endpoint | Method | Status | Evidence |
|---|---|---|---|
| `/openapi.json` | GET | Supported | `crates/arco-uc/src/routes/openapi.rs` |
| `/catalogs` | GET | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/catalogs.rs` |
| `/catalogs` | POST | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/catalogs.rs` |
| `/catalogs/{name}` | GET | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/catalogs.rs` |
| `/catalogs/{name}` | DELETE | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/catalogs.rs` |
| `/schemas` | GET | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/schemas.rs` |
| `/schemas` | POST | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/schemas.rs` |
| `/schemas/{full_name}` | GET | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/schemas.rs` |
| `/schemas/{full_name}` | DELETE | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/schemas.rs` |
| `/tables` | GET | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/tables.rs` |
| `/tables` | POST | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/tables.rs` |
| `/tables/{full_name}` | GET | Implemented (preview + catalog-native fallback) | `crates/arco-uc/src/routes/tables.rs` |
| `/tables/{full_name}` | DELETE | Implemented (tenant/workspace scoped) | `crates/arco-uc/src/routes/tables.rs` |
| `/delta/preview/commits` | GET | Implemented (arco-delta state/log semantics + backfill filtering) | `crates/arco-uc/src/routes/delta_commits.rs` |
| `/delta/preview/commits` | POST | Implemented (coordinator-backed idempotent commit flow) | `crates/arco-uc/src/routes/delta_commits.rs` |
| `/temporary-path-credentials` | POST | Implemented (GCS-first, no AWS/Azure vending) | `crates/arco-uc/src/routes/credentials.rs` |
| `/permissions/{securable_type}/{full_name}` | GET | Implemented (read-only scope A) | `crates/arco-uc/src/routes/permissions.rs` |

## Partial / preview-only

| Endpoint | Method | Status | Evidence |
|---|---|---|---|
| `/temporary-table-credentials` | POST | Preview scaffold (`501`) | `crates/arco-uc/src/routes/credentials.rs` |
| `/catalogs/{name}` | PATCH | Preview scaffold (`501`) | `crates/arco-uc/src/routes/catalogs.rs` |
| `/schemas/{full_name}` | PATCH | Preview scaffold (`501`) | `crates/arco-uc/src/routes/schemas.rs` |

## Not supported

All remaining endpoints in the pinned Unity Catalog OSS inventory remain out of scope for this Q2 core slice.

Reference inventory:
`docs/plans/2026-02-04-unity-catalog-openapi-inventory.md`

## Evidence checks

- UC crate verification:
  `cargo test -p arco-uc`
- Contract compliance:
  `cargo test -p arco-uc --test openapi_compliance`
- API mount/auth wiring:
  `cargo test -p arco-api test_unity_catalog_`
  `cargo test -p arco-api test_unity_catalog_mount_gating_and_legacy_path_hidden`
- Delta coordinator invariants:
  `cargo test -p arco-delta`
- Engine smoke suites:
  `cargo test -p arco-integration-tests --test delta_engine_smoke spark_engine_smoke_uc_and_native_delta`
  `cargo test -p arco-integration-tests --test delta_engine_smoke delta_rs_engine_smoke_uc_and_native_delta`
  `cargo test -p arco-integration-tests --test delta_engine_smoke pyspark_engine_smoke_uc_and_native_delta`
- Inventory drift gate:
  `cargo xtask uc-openapi-inventory`
- Parity evidence gate:
  `cargo xtask parity-matrix-check`
