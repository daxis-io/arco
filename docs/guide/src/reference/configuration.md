# Configuration Reference

This page summarizes the main runtime configuration surfaces. Environment-variable specifics live with each service implementation.

## API Service

Canonical config source:

- `crates/arco-api/src/config.rs`

Representative variables include:

- Core runtime: `ARCO_HTTP_PORT`, `ARCO_GRPC_PORT`, `ARCO_ENVIRONMENT`, `ARCO_API_PUBLIC`
- Auth/JWT: `ARCO_JWT_*`
- Storage and compaction wiring: `ARCO_STORAGE_BUCKET`, `ARCO_COMPACTOR_URL`, `ARCO_ORCH_COMPACTOR_URL`
- Catalog feature flags: `ARCO_ICEBERG_*`, `ARCO_UNITY_CATALOG_*`

## Flow Services

Dispatcher/sweeper/compactor binaries consume environment-driven configuration under:

- `crates/arco-flow/src/bin/arco_flow_dispatcher.rs`
- `crates/arco-flow/src/bin/arco_flow_sweeper.rs`
- `crates/arco-flow/src/bin/arco_flow_compactor.rs`

Common required values include tenant/workspace IDs, storage bucket, target URLs, and cloud queue settings.

## CLI

Canonical config source:

- `crates/arco-cli/src/lib.rs`

Primary variables:

- `ARCO_API_URL`
- `ARCO_WORKSPACE_ID`
- `ARCO_API_TOKEN`

## Compactor

Canonical config source:

- `crates/arco-compactor/src/main.rs`

Service mode and one-shot mode both support environment-based defaults for tenant/workspace scope, storage, cadence, and health thresholds.

## Repository Automation

- `tools/xtask/src/main.rs` contains repository checks, ADR/parity validation, and hygiene gates.
