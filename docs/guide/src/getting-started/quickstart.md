# Quick Start

This quickstart validates the repository, then runs the main quality gates used by contributors.

## 1. Build and Test

```bash
cargo check --workspace --all-features
cargo test --workspace --all-features --exclude arco-flow --exclude arco-api
```

## 2. Run Repository Gates

```bash
cargo xtask adr-check
cargo xtask parity-matrix-check
cargo xtask repo-hygiene-check
```

## 3. Build Documentation

```bash
cd docs/guide
mdbook build
```

## 4. Inspect API Contract

The OpenAPI contract is tracked at:

- `crates/arco-api/openapi.json`

Use this file as the canonical REST contract snapshot in review and CI workflows.
