# Catalog

The catalog domain tracks data assets, schemas, lineage, and governance metadata.

## What the Catalog Stores

- Asset identity and namespace metadata.
- Schema and contract metadata.
- Lineage edges and traversal-friendly projections.
- Search indexes optimized for metadata discovery.

## Storage Characteristics

- Immutable files and versioned snapshots.
- Manifest-driven discovery of latest readable state.
- Append-first operational signals with compaction to query-efficient tables.

## Query Model

- Server-side querying is centered on DataFusion.
- Browser-oriented read paths are enabled through scoped signed URL workflows.
- Catalog reads are designed to remain deterministic and auditable.

## Security and Isolation

- Tenant/workspace scoped paths are mandatory.
- Access controls are enforced before URL minting and data-path exposure.
- Existence-privacy posture is defined in ADRs and can be tightened without redesigning the storage model.

## Canonical References

- `docs/adr/adr-001-parquet-metadata.md`
- `docs/adr/adr-005-storage-layout.md`
- `docs/adr/adr-018-tier1-write-path.md`
- `docs/adr/adr-019-existence-privacy.md`
