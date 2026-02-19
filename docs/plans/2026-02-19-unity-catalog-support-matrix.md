# Unity Catalog Support Matrix (Q1 Baseline)

Last updated: 2026-02-19

## Scope and gating

- Mount prefix: `/api/2.1/unity-catalog`
- Feature gate: `unity_catalog.enabled` (`ARCO_UNITY_CATALOG_ENABLED`)
- Pinned contract source:
  `crates/arco-uc/tests/fixtures/unitycatalog-openapi.yaml`

## Supported

| Endpoint | Method | Status | Evidence |
|---|---|---|---|
| `/openapi.json` | GET | Supported | `crates/arco-uc/src/routes/openapi.rs` |

## Preview

| Endpoint | Method | Status | Evidence |
|---|---|---|---|
| `/catalogs` | GET | Preview (scaffolded) | `crates/arco-uc/src/routes/catalogs.rs` |
| `/catalogs` | POST | Preview (scaffolded) | `crates/arco-uc/src/routes/catalogs.rs` |
| `/schemas` | GET | Preview (scaffolded) | `crates/arco-uc/src/routes/schemas.rs` |
| `/schemas` | POST | Preview (scaffolded) | `crates/arco-uc/src/routes/schemas.rs` |
| `/tables` | GET | Preview (scaffolded) | `crates/arco-uc/src/routes/tables.rs` |
| `/tables` | POST | Preview (scaffolded) | `crates/arco-uc/src/routes/tables.rs` |
| `/delta/preview/commits` | GET | Preview (scaffolded) | `crates/arco-uc/src/routes/delta_commits.rs` |
| `/delta/preview/commits` | POST | Preview (scaffolded) | `crates/arco-uc/src/routes/delta_commits.rs` |
| `/temporary-table-credentials` | POST | Preview (scaffolded) | `crates/arco-uc/src/routes/credentials.rs` |
| `/temporary-path-credentials` | POST | Preview (scaffolded) | `crates/arco-uc/src/routes/credentials.rs` |

## Not supported

All other endpoints in the pinned Unity Catalog OSS OpenAPI inventory are currently
not supported in Arco and are intentionally outside the Q1/Q2 v1 target set.

Reference inventory:
`docs/plans/2026-02-04-unity-catalog-openapi-inventory.md`

## Evidence checks

- Conformance test (v1 preview operation set):
  `cargo test -p arco-uc --test openapi_compliance`
- UC router/auth smoke:
  `cargo test -p arco-api test_unity_catalog_`
- Inventory drift gate:
  `cargo xtask uc-openapi-inventory`
