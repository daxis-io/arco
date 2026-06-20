# Quick Start

This quickstart validates the repository, then runs the main quality gates used by contributors.

## 1. Build and Test

```bash
cargo check --workspace --all-features
cargo test --workspace --all-features --exclude arco-flow --exclude arco-api
```

## 2. Run Local Pipeline UAT

```bash
bash scripts/run_local_pipeline_uat.sh
```

This no-cloud UAT writes sample Parquet data, registers and queries catalog
tables, stages and commits a Delta table log, deploys a manifest, dispatches a
task, completes the callback, and verifies the final run and task state. It
does not use GCP, Cloud Run, Cloud Scheduler, Cloud Tasks, or GCS.

To run the same no-cloud UAT inside a Linux container:

```bash
bash scripts/run_local_pipeline_uat_container.sh
```

This requires a running Docker daemon.

## 3. Run Repository Gates

```bash
cargo xtask adr-check
cargo xtask parity-matrix-check
cargo xtask repo-hygiene-check
```

## 4. Build Documentation

```bash
cd docs/guide
mdbook build
```

## 5. Inspect API Contract

The OpenAPI contract is tracked at:

- `crates/arco-api/openapi.json`

Use this file as the canonical REST contract snapshot in review and CI workflows.
