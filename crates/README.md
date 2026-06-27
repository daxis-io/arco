# Arco Crates

## Crate Map

| Crate | Status | Release role | Description |
|-------|--------|--------------|-------------|
| `arco-core` | Alpha | Library | Shared primitives: tenant/workspace scope, IDs, errors, storage traits, task tokens |
| `arco-catalog` | Alpha | Library | Catalog authority: Tier-1 snapshots, Tier-2 events, lineage, search, authz helpers |
| `arco-delta` | Alpha | Library | Delta Lake commit coordination and log helpers |
| `arco-flow` | Alpha | Library + binaries | Orchestration domain, controllers, dispatch contracts, and flow service binaries |
| `arco-api` | Alpha | Service | HTTP/gRPC composition layer for API, query, UC facade mounting, and internal routes |
| `arco-proto` | Alpha | Library | Generated protobuf types for Arco control-plane contracts |
| `arco-worker-contract` | Alpha | Library | Versioned worker dispatch and callback protocol types |
| `arco-compactor` | Alpha | Service | Catalog compactor binary deployed as a Cloud Run service |
| `arco-cli` | Alpha | CLI | Command-line interface for local and operational workflows |
| `arco-iceberg` | Alpha | Library | Iceberg REST Catalog integration and CAS-based commit support |
| `arco-uc` | Alpha | Library | Unity Catalog OSS API facade and compatibility support registry |
| `arco-test-utils` | Alpha | Test-only | Shared test utilities; `publish = false` |
| `arco-integration-tests` | Alpha | Test-only | Cross-crate integration test harness; `publish = false` |

## Deployment Notes

- `arco-compactor` is deployed by Terraform as
  `google_cloud_run_v2_service.compactor`.
- Flow control-plane binaries from `arco-flow` are deployed as separate Cloud
  Run services when their images and tenant/workspace configuration are set.

## Boundary Rules

1. **Only `arco-core` defines shared primitives** - no utility dumping elsewhere
2. **Cross-crate interaction via contracts** - Rust types + protobuf + integration tests
3. **No implicit coupling** - explicit versioned interfaces only

## Publishability Criteria

A crate is publishable when it has:
- `#![deny(missing_docs)]` enabled
- Documented MSRV in Cargo.toml
- Stability promise in crate-level docs
- Passing CI (lint, test, doc)

## Feature Flags

| Flag | Purpose |
|------|---------|
| `default` | Minimal production-safe defaults |
| `full` | All optional integrations |
| `gcp` | Google Cloud integrations |
| `aws` | AWS integrations |
| `test-utils` | Testing utilities (dev only) |
