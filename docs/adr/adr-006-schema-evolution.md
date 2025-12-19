# ADR-006: Parquet Schema Evolution Policy

## Status
Accepted

## Context

Arco stores catalog metadata as Parquet files that clients read via DuckDB, Datafusion, and other query engines. Once schemas are published:
- Clients may cache schema information
- Query engines make assumptions about field presence and types
- Silent schema changes cause query failures or data corruption

Schema evolution is the #1 source of "works on my machine" production failures. We need explicit rules that prevent breaking changes while allowing the schema to grow.

## Decision

### Evolution Rules

All Parquet schema changes must follow these rules:

| Rule | Description | Example |
|------|-------------|---------|
| **Additive-only** | New fields must be nullable | Add `description: Option<String>` |
| **No field removal** | Deprecated fields remain in schema | Mark with `deprecated_` prefix |
| **No type changes** | Field types are immutable | Cannot change `Int32` → `Int64` |
| **Nullable preserved** | Nullable → non-nullable is breaking | Cannot add NOT NULL constraint |

### Golden File Testing

Each Parquet table has a golden schema file checked into the repository:

```
crates/arco-catalog/tests/golden_schemas/
├── namespaces.schema.json
├── tables.schema.json
├── columns.schema.json
└── lineage_edges.schema.json
```

CI tests compare current schemas against golden files and fail on:
- Missing fields (removal)
- Type mismatches
- Nullable → non-nullable changes
- Non-nullable new fields

### Version Gates for Breaking Changes

When breaking changes are unavoidable (major version bumps):

1. **Announce deprecation** in release notes (minimum 2 releases ahead)
2. **Add version gate** in manifest (`schema_version: 2`)
3. **Support both versions** during migration window
4. **Remove old version** only after migration period

### Schema Version in Manifests

Each domain manifest includes schema version:

```json
{
  "domain": "catalog",
  "schema_version": 1,
  "snapshot_version": 42,
  ...
}
```

Readers check `schema_version` and fail fast if incompatible.

## Consequences

### Positive
- Clients can rely on schema stability
- Query failures are caught at development time, not production
- Clear migration path for major changes
- Golden files serve as documentation

### Negative
- Cannot easily remove deprecated fields (schema bloat)
- Breaking changes require multi-release migration
- Golden files must be updated when adding fields

### Neutral
- Developers must run `cargo test --test schema_contracts` before merging
- CI gate ensures compliance automatically

## Related

- [ADR-001: Parquet-first Metadata Storage](adr-001-parquet-metadata.md)
- [ADR-005: Canonical Storage Layout](adr-005-storage-layout.md)
- Schema contract tests: `crates/arco-catalog/tests/schema_contracts.rs`
