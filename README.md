# Arco

Lakehouse catalog and orchestration built on object storage.

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.88%2B-orange.svg)](https://www.rust-lang.org)

Arco manages table metadata, tracks lineage, and schedules data pipelines.
It publishes catalog and run metadata as Parquet files for your query engine.
You supply the query engine and task workers.

**Status:** Incubating. Arco is not yet production ready, and APIs may change.

- **Catalog:** Table identity, schemas, and storage locations. New table
  registrations default to Delta Lake; Iceberg and plain Parquet have
  [documented support levels](docs/guide/src/reference/control-plane-scope.md).
- **Orchestration:** Planning, scheduling, and run history. External workers
  execute tasks and report results to Arco.
- **Metadata:** Catalog, lineage, and run history in open Parquet files.
  The legacy catalog API provides scoped signed URLs for published files.

## Getting started

You need Rust 1.88+ and the Protocol Buffers compiler (`protoc`).

Clone, build, and test:

```bash
git clone https://github.com/daxis-io/arco.git
cd arco

cargo build --workspace
cargo test --workspace
```

Follow the [quick start](docs/guide/src/getting-started/quickstart.md) to run a
local pipeline that registers tables, commits Delta data, and executes a task.

## Documentation

- [Installation](docs/guide/src/getting-started/installation.md)
- [Architecture](docs/guide/src/concepts/architecture.md)
- [Published metadata](docs/guide/src/reference/system-catalog.md)
- [Integration guide](docs/guide/src/reference/integration-profile-030.md)
- [Flow dispatcher example](docs/runbooks/operator-owned-flow-dispatcher.md)
- [API Reference](docs/guide/src/reference/api.md)
- [Documentation Index](docs/guide/src/reference/documentation-map.md)

To browse the guide locally:

```bash
cargo install mdbook --version 0.4.52 --locked
cd docs/guide && mdbook build --open
```

## Contributing

- [Contributing guide](CONTRIBUTING.md)
- [Code of conduct](CODE_OF_CONDUCT.md)
- [Security policy](SECURITY.md)
- [Community & support](COMMUNITY.md)

For protobuf changes, follow the
[compatibility policy](docs/guide/src/reference/schema-evolution-policy.md)
and run `cargo xtask proto-breaking-check`.

## License

Apache License 2.0 - see [LICENSE](LICENSE) and [NOTICE](NOTICE).
