# Catalog

The catalog domain is Arco's file-native catalog and metastore for open lakehouse
table formats. It tracks table identity, schemas, locations, lineage, and
search-oriented metadata today. Delta Lake is the first-class managed table
format and the default for new registrations; Iceberg and plain Parquet are
explicit catalog surfaces with support levels tied to implemented Arco-native
state.

Its product boundary is native to Arco: UC-compatible APIs are adapters over
Arco-owned state, not the source of the architecture.

Arco already proves authoritative control-plane behavior for:

- catalogs, schemas, tables, and columns
- table-format contracts for Delta Lake, Iceberg, and plain Parquet
- lineage projections
- search projections derived from current catalog state
- the initial native metastore replay/projection kernel for stable-ID object
  folding and redacted generic metastore projection rows

Broader governance scope remains narrower than the highest-level architectural
framing sometimes implies. Grants, permissions, credentials, and policy-style
metadata now have contract and early kernel coverage, but they are not yet
production-backed through writer APIs, compiled authorization, credential
vending, compatibility routes, or system tables. Use
`docs/guide/src/reference/control-plane-scope.md` when describing
implementation status.

The broader catalog product contract is documented in:

- `docs/adr/adr-037-arco-catalog-product-surface.md`
- `docs/adr/adr-038-catalog-threat-model.md`
- `docs/adr/adr-039-catalog-consistency-model.md`
- `docs/guide/src/reference/catalog-privilege-matrix.md`
- `docs/guide/src/reference/catalog-api-contract.md`
- `docs/guide/src/reference/credential-vending-security.md`

## What the Catalog Stores

- Asset identity and namespace metadata.
- Schema and contract metadata.
- Lineage edges and traversal-friendly projections.
- Search indexes optimized for metadata discovery.
- Planned product domains for grants, storage governance, credentials, volumes,
  views, functions, models, governance attachments, and access audit.

## Storage Characteristics

- Immutable files and versioned snapshots.
- Manifest-driven discovery of latest readable state.
- Append-first operational signals with compaction to query-efficient tables.

## Query Model

- Server-side querying is centered on DataFusion.
- Browser-oriented read paths are enabled through scoped signed URL workflows.
- Catalog reads are designed to remain deterministic and auditable.

Catalog reads stay pointer-first: `/api/v1/query` is the initial SQL surface
for `system.*` tables, and those tables are queryable projections over
manifest-selected Parquet artifacts rather than the commit point for control-plane truth.

## Security and Isolation

- Tenant/workspace scoped paths are mandatory.
- Access controls are enforced before URL minting and data-path exposure.
- Existence-privacy posture is defined in ADRs and can be tightened without redesigning the storage model.
- Future grants, credential vending, governed paths, and sensitive system tables
  must deny by default, redact by schema, and audit allow and deny decisions.

## Canonical References

- `docs/adr/adr-001-parquet-metadata.md`
- `docs/adr/adr-005-storage-layout.md`
- `docs/adr/adr-018-tier1-write-path.md`
- `docs/adr/adr-019-existence-privacy.md`
