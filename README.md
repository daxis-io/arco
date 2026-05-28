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
- Buf 1.70.0

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

Arco is still alpha/beta software. The old `arco.v1` protobuf package was
intentionally removed and replaced by domain-aligned packages:
`arco.common.v1`, `arco.catalog.v1`, `arco.orchestration.v1`, and
`arco.controlplane.v1`.

Additional breaking `v1` proto changes may happen before the stable public API
freeze, but they must be grouped into a documented hard-cut window and followed
by a regenerated `proto-baselines/post-hard-cut-v1.binpb`. Outside an explicit
hard-cut window, `v1` changes must pass `cargo xtask proto-breaking-check` and
preserve generated source, package/service, binary, and ProtoJSON compatibility
with that frozen baseline.

Current hard-cut migration note: `RegisterTableOp.format` is optional. Omit it
for the Delta Lake default; do not send `TABLE_FORMAT_UNSPECIFIED` on
`RegisterTableOp`.

HTTP protobuf transaction routes require a message-qualified content type such
as `application/x-protobuf; proto=arco.controlplane.v1.ApplyCatalogDdlRequest`.
Generic or legacy protobuf bodies are rejected before decode so old wire shapes
cannot be reinterpreted as new mutations.

## Contributing

- [Contributing guide](CONTRIBUTING.md)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [Security policy](SECURITY.md)
- [Community & support](COMMUNITY.md)

## License

Apache License 2.0 - see [LICENSE](LICENSE) and [NOTICE](NOTICE).
