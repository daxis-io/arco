# Arco

**File-native lakehouse catalog and orchestration.** A catalog and metastore
for open table formats, with Delta Lake as the first-class managed format.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)
![Status](https://img.shields.io/badge/status-incubating-yellow.svg)

> Arco is incubating and not yet production ready. APIs may change. Early feedback welcome.

## What is Arco?

Arco stores catalog and orchestration metadata as **Parquet files on object
storage** no always on database, no proprietary catalog service. Query your
metadata with SQL the same way you query your data.

At the catalog layer, Arco manages table identity, locations, schemas, lineage,
and operational metadata for open lakehouse table formats. New Arco table
registrations default to Delta Lake; Iceberg and plain Parquet are explicit
catalog surfaces with compatibility and governance support growing over time.

## Why Arco?

- **No catalog server to operate** - metadata lives in object storage; engines
  read it directly via signed URLs.
- **Query metadata with SQL** - catalog, lineage, and run history are exposed
  as `system.*` tables.
- **Real lineage** - captured from actual runs, not guessed from SQL parsing.
- **Multi-tenant by design** - isolation is enforced at storage layout, service
  boundaries, and test gates.
- **Open table formats** - Delta Lake is the primary managed format; Iceberg
  and plain Parquet are modeled in catalog contracts.

## Get Started

### Prerequisites

- Rust 1.88+ (Edition 2024)
- Protocol Buffers compiler (`protoc`)

### Build and test

```bash
git clone https://github.com/daxis-io/arco.git
cd arco

cargo build --workspace
cargo test --workspace
```

### Browse the docs

```bash
cargo install mdbook --version 0.4.52 --locked
cd docs/guide && mdbook build --open
```

Or jump straight to:

- [Quick Start](docs/guide/src/getting-started/quickstart.md)
- [Architecture](docs/guide/src/concepts/architecture.md)
- [System Catalog](docs/guide/src/reference/system-catalog.md)

## How it fits together

```
arco-api        HTTP/gRPC entry point (read only SQL via DataFusion)
arco-catalog    Table format catalog, lineage, Parquet metadata snapshots
arco-flow       Planning, scheduling, run state
arco-compactor  Tier-2 event consolidation
arco-proto      Cross-language protobuf contracts
arco-core       Shared primitives (tenant context, IDs, errors)
```

Task execution runs in external workers via a canonical dispatch envelope. The browser query path uses DuckDB-WASM against signed URLs - no always-on infrastructure required.

## Proto compatibility

The pre-freeze hard cut is complete. The current `arco.*.v1` packages are the
durable public proto surface represented by the frozen post-cut baseline. New
`v1` changes must be additive and must preserve binary and ProtoJSON
compatibility. Run `cargo xtask proto-breaking-check` before merging proto
changes.

## Contributing

- [Contributing guide](CONTRIBUTING.md)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [Security policy](SECURITY.md)
- [Community & support](COMMUNITY.md)

## License

Apache License 2.0 - see [LICENSE](LICENSE) and [NOTICE](NOTICE).
