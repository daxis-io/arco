# Arco

> **Incubating Project** - Arco is under active development and not yet ready for production use. APIs may change without notice. We welcome early feedback and contributions.

**Serverless lakehouse infrastructure** - A file-native catalog and execution-first orchestration layer for modern data platforms.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
![Status](https://img.shields.io/badge/status-incubating-yellow.svg)

---

## Overview

Arco unifies a **file-native catalog** and an **execution-first orchestration** layer into one operational metadata system. It stores metadata as immutable, queryable files on object storage and treats deterministic planning, replayable history, and explainability as product requirements.

### Key Differentiators

| Feature | Description |
|---------|-------------|
| **Metadata as files** | Parquet-first storage for catalog and operational metadata, optimized for direct SQL access |
| **Query-native reads** | Browser and server query engines read metadata directly via signed URLs, eliminating always-on infrastructure |
| **Lineage-by-execution** | Lineage captured from real runs (inputs/outputs/partitions), not inferred from SQL parsing |
| **Two-tier consistency** | Strong consistency for DDL; eventual consistency for high-volume operational facts |
| **Tenant isolation** | Enforced at storage layout, service boundaries, and test gates |

## Documentation

- Documentation map: `docs/README.md`
- Canonical guide/reference docs (mdBook): `docs/guide/src/`
- Architecture decisions: `docs/adr/README.md`
- Operations runbooks: `docs/runbooks/`
- Release process: `RELEASE.md`
- Evidence policy: `docs/guide/src/reference/evidence-policy.md`

## Architecture

```
arco/
├── crates/
│   ├── arco-core/       # Core abstractions: types, storage traits, tenant context
│   ├── arco-catalog/    # Catalog service: registry, lineage, search, Parquet storage
│   ├── arco-flow/       # Orchestration: planning, scheduling, state machine
│   ├── arco-api/        # HTTP/gRPC composition layer
│   ├── arco-proto/      # Protobuf definitions
│   └── arco-compactor/  # Compaction binary for Tier 2 events
├── proto/               # Canonical .proto files
├── python/              # Python SDK
└── docs/                # mdBook docs + ADRs + audits
```

## Engine Boundaries (ADR-032)

Arco enforces explicit engine responsibilities in split-services deployments:

- `arco-api` and `arco-flow` are control-plane services.
- DataFusion endpoints (`/api/v1/query`, `/api/v1/query-data`) are read-only (`SELECT`/`CTE`).
- Compactors are sole writers for state/snapshot Parquet projections.
- Browser query path uses DuckDB-WASM via signed URLs only.
- ETL compute runs in external workers via canonical `WorkerDispatchEnvelope`.

Current cycle non-goals:

- No in-process ETL engine.
- No Spark/dbt/Flink adapter implementation.

## Quick Start

### Prerequisites

- Rust 1.85+ (Edition 2024)
- Protocol Buffers compiler (`protoc`)
- mdBook (`cargo install mdbook --version 0.4.52 --locked`) for local docs builds

### Build

```bash
# Clone the repository
git clone https://github.com/daxis-io/arco.git
cd arco

# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Check formatting and lints
cargo fmt --all --check
cargo clippy --workspace --all-features -- -D warnings
```

### Local CI/Gate Parity

```bash
cargo xtask doctor
cargo xtask adr-check
cargo xtask verify-integrity
cargo xtask parity-matrix-check
cargo xtask repo-hygiene-check
cargo xtask uc-openapi-inventory
git diff --exit-code -- docs/guide/src/reference/unity-catalog-openapi-inventory.md

# Build docs
cd docs/guide && mdbook build
```

## Crates

| Crate | Description | Status |
|-------|-------------|--------|
| `arco-core` | Shared primitives: tenant context, IDs, errors, storage traits | Alpha |
| `arco-catalog` | Catalog domain: asset registry, lineage, Parquet snapshots | Alpha |
| `arco-flow` | Orchestration domain: planning, scheduling, run state | Alpha |
| `arco-api` | HTTP/gRPC composition layer | Alpha |
| `arco-proto` | Protobuf definitions for cross-language contracts | Alpha |
| `arco-compactor` | Compaction binary for Tier 2 event consolidation | Alpha |

## Contributing

- Contribution guidelines: [CONTRIBUTING.md](CONTRIBUTING.md)
- Community pathways: [COMMUNITY.md](COMMUNITY.md), [SUPPORT.md](SUPPORT.md)
- Code of conduct: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

### Development

```bash
cargo fmt --all --check
cargo clippy --workspace --all-features -- -D warnings
cargo test --workspace --all-features --exclude arco-flow --exclude arco-api
```

## Security

For security vulnerabilities, please see [SECURITY.md](SECURITY.md).

## License

Licensed under the Apache License, Version 2.0.

- License text: [LICENSE](LICENSE)
- Attribution notice: [NOTICE](NOTICE)
