# Schema Evolution Policy

This page summarizes compatibility rules across the catalog product surface.

## Protobuf

Arco's alpha/beta proto surface intentionally replaced the original `arco.v1`
package with domain-aligned packages: `arco.common.v1`, `arco.catalog.v1`,
`arco.orchestration.v1`, and `arco.controlplane.v1`.

During alpha/beta, additional breaking `v1` changes can happen without cutting
`v2` only as an explicit hard-cut window. That window must document migration
impact, regenerate `proto-baselines/post-hard-cut-v1.binpb`, and keep
`cargo xtask proto-breaking-check` passing afterward.

Outside an explicit hard-cut window, protobuf changes must be additive, preserve
generated source, package/service, binary, and ProtoJSON compatibility, and pass
`cargo xtask proto-breaking-check` against the frozen baseline. Once Arco declares a stable public API, breaking
reshapes require future `v2` packages.

Current hard-cut migration note: `arco.catalog.v1.RegisterTableOp.format` is an
optional enum. Producers should omit the field for the Delta Lake default and
must not send `TABLE_FORMAT_UNSPECIFIED` on `RegisterTableOp`.

HTTP protobuf transaction routes require message-qualified content types such as
`application/x-protobuf; proto=arco.controlplane.v1.ApplyCatalogDdlRequest` so
legacy or generic protobuf bodies fail closed before decoding.

## Parquet And Arrow

Projection schemas are public contracts once exposed through readers, system
tables, discovery APIs, or compatibility adapters.

- Add nullable fields instead of changing existing field meaning.
- Do not remove, rename, reorder semantically, or narrow types in exposed
  projections.
- Include schema version and ledger watermark fields for new product
  projections.
- Maintain golden schema fixtures for implemented projections.
- Exclude sensitive fields by schema, not only by route filtering.

## OpenAPI

OpenAPI is a contract artifact. Native and compatibility routes need snapshot
coverage before they are marked production-backed. Route groups must carry a
compatibility label and document unsupported fields or lifecycle states.

OpenAPI changes are compatible only when they add optional request fields,
nullable response fields, new enum values documented as forward-compatible, or
new routes marked `planned` or `scaffolded`. Removing routes, narrowing types,
renaming fields, making optional fields required, or changing error code
semantics requires an explicit breaking-version plan.

## System Tables

System-table schemas are read-only public APIs. New system tables require:

- explicit allowlist registration
- workspace scoping
- redaction review
- schema compatibility tests
- freshness or watermark columns where practical
- docs that state whether the table is implemented, partial, or planned

## Compatibility Gates

| Surface | Required gate before production-backed status |
|---|---|
| Protobuf | `cargo xtask proto-breaking-check` and generated fixture update when needed |
| Parquet/Arrow projection | Golden schema fixture, schema version, ledger watermark, and redaction review |
| OpenAPI | Snapshot or diff evidence plus route compatibility label |
| System table | Allowlist entry, schema check, ACL/redaction test, and freshness/watermark behavior |
| Docs examples | Secret-scan/redaction review for tokens, keys, raw credential payloads, and private paths |

Compatibility checks should fail closed: if a tool cannot determine whether a
change is safe, the change is treated as requiring human review and a versioning
note.

## Fixtures And Examples

Fixtures, docs, errors, logs, traces, and generated examples must not include
secret material or raw credential payloads. Redaction is part of compatibility.
