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
└── docs/                # Documentation and ADRs
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
cargo fmt --check
cargo clippy --workspace -- -D warnings
```

### Example Usage

```rust
use arco_core::prelude::*;

// Create a tenant context
let tenant = TenantId::new("acme-corp")?;

// Generate a unique asset ID
let asset_id = AssetId::generate();
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

## Performance Targets

| Operation | P95 Target |
|-----------|------------|
| Catalog table lookup | < 50ms |
| Full catalog scan (1000 tables) | < 500ms |
| Lineage traversal (5 hops) | < 100ms |
| Plan generation (100 tasks) | < 200ms |

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development

```bash
# Format code
cargo fmt

# Run clippy
cargo clippy --workspace --all-features -- -D warnings

# Run tests with coverage
cargo llvm-cov --workspace

# Check supply chain security
cargo deny check
```

## Security

For security vulnerabilities, please see [SECURITY.md](SECURITY.md).

## License

Licensed under the Apache License, Version 2.0.

- License text: [LICENSE-APACHE](LICENSE-APACHE)
- Project license notice: [LICENSE](LICENSE)
- Attribution notice: [NOTICE](NOTICE)

## Acknowledgments

Arco is developed by [Daxis](https://daxis.io) and open sourced to advance the data ecosystem.
