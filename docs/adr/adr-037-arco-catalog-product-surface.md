# ADR-037: Arco Catalog Product Surface

## Status

Proposed

## Context

Arco already has authoritative catalog/schema/table state, explicit table-format
contracts for Delta Lake, Iceberg, and plain Parquet, lineage/search
publication, orchestration transactions, Delta commit coordination, and an
allowlisted `system` catalog for read-only operational metadata. The broader
catalog product surface still needs explicit boundaries before grants,
credentials, volumes, functions, models, governance metadata, and compatibility
adapters become production-backed.

Unity Catalog is useful prior art for product scope and interoperability. It is
not Arco's architectural authority. Arco's native contract is ledger-backed
state, deterministic replay, projection compaction, immutable snapshot
publication, and fenced pointer movement.

## Decision

Arco's catalog product surface is native and object-store-backed. It is a
catalog and metastore for open lakehouse table formats, with Delta Lake as the
first-class managed table format and the default for new registrations.
Compatibility APIs expose Arco state to outside engines, but they must preserve
Arco's native invariants.

The catalog product owns these object families:

- catalogs, schemas, tables, table formats, columns, constraints, and views
- grants, principals, groups, ownership, and inherited permissions
- storage credentials, service credentials, external locations, managed roots,
  workspace bindings, and governed paths
- volumes, functions, registered models, and model versions
- governance attachments such as tags, classifications, glossary terms, data
  domains, stewards, and policy placeholders
- lineage, discovery summaries, audit events, and system-table projections

Roadmap object families such as external service connections, shares,
providers, recipients, data products, and business domains are compatibility
and product-planning surfaces only until Arco has native events, projections,
authorization, audit, docs, tests, and scorecard evidence for them.

Stable object IDs are enforcement keys. Names are mutable aliases used for
lookup and compatibility. Rename must not change object ID, grants, lineage
bindings, storage bindings, governance attachments, or audit identity.

Authoritative mutations append immutable events, participate in deterministic
hashing and replay, fold into typed state, compact into additive projection
schemas, and become visible only after immutable snapshot publication plus
pointer movement. Readers use published projections or compiled views. Search,
lineage, discovery, and system tables are derived surfaces, not enforcement
inputs.

Route crates are adapters. Native, UC-compatible, Iceberg-compatible, SQL, and
internal route groups must call Arco-owned domain APIs for identity,
authorization, storage governance, credential vending, mutation, projection, and
audit behavior. Route-local policy conditionals are allowed only as request
shape validation before calling the domain API.

Table-format behavior is part of that native contract. New Arco table
registrations default to Delta Lake. Iceberg-facing APIs and plain Parquet table
metadata are product/compatibility surfaces that must not bypass stable IDs,
publication, authorization, storage governance, or compatibility labels.

Every compatibility route group carries a support level:

- `native`: Arco-native contract and semantics.
- `compatible-exact`: external semantics are preserved for supported fields.
- `compatible-partial`: route shape exists with documented limits.
- `scaffolded`: route exists for discovery or future compatibility only.
- `planned`: documented intent with no route behavior.

The completion gate for any object family is:

1. Stable object identity and parent hierarchy exist.
2. Authoritative mutation events exist.
3. Deterministic replay and typed state folds exist.
4. Projection schemas are additive, versioned, watermarked, and redacted.
5. Reader and writer APIs exist.
6. Authorization and audit participate in every read, list, mutate, query, and
   credential-mint path.
7. Native docs, compatibility labels, schema tests, deny-path tests, and
   scorecard evidence exist.

## Consequences

- A catalog domain is not `Implemented` until mutation, replay, projection,
  reader/writer APIs, authorization, audit, tests, docs, and scorecard evidence
  all exist.
- System tables must be explicit, allowlisted, redacted, workspace-scoped, and
  freshness-watermarked.
- Compatibility adapters must not bypass stable-ID enforcement, storage
  governance, credential scope, or deny-by-default authorization.
- Future implementation work starts from this product boundary rather than from
  UC route parity alone.
