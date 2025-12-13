# Arco Crates

## Crate Map

| Crate | Status | Publishable | Description |
|-------|--------|-------------|-------------|
| `arco-core` | Alpha | Yes | Shared primitives: tenant context, IDs, errors, storage traits |
| `arco-catalog` | Alpha | Yes | Catalog domain: Tier 1 snapshots, Tier 2 events, lineage |
| `arco-flow` | Alpha | Yes | Orchestration: planning, scheduling, run state |
| `arco-api` | Alpha | Yes | HTTP/gRPC composition layer |
| `arco-proto` | Alpha | Yes | Protobuf definitions (prost-generated) |
| `arco-compactor` | Alpha | No | Compaction binary (Cloud Function entrypoint) |

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
