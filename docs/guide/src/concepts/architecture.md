# Architecture

Arco is a file-native metadata platform with two tightly integrated domains:

- Catalog: catalog and metastore for open lakehouse table formats, with Delta
  Lake as the first-class managed format.
- Orchestration: deterministic planning, execution state, and replay-safe event flows.

## Core Principles

- Metadata as immutable files on object storage.
- Open table-format contracts, with new table registrations defaulting to
  Delta Lake and Iceberg/Parquet exposed through explicit support levels.
- Query-native reads (DataFusion and browser-friendly read paths).
- Deterministic planning and replayable execution history.
- Tenant isolation at storage path, API, and policy boundaries.
- Compatibility APIs are adapters over native Arco state.

## Domain Model

- Tier 1 metadata (low-frequency, strongly consistent): schema and implemented control-plane state such as catalog DDL, lineage/search publication, and orchestration transactions.
- Tier 2 metadata (high-frequency, append-first): execution and operational events.
- Compaction materializes query-efficient snapshots from append-first logs.
- Catalog product domains such as grants, credentials, volumes, functions,
  models, and governance metadata follow the same ledger -> projection ->
  immutable snapshot -> pointer-publication model when implemented.

## Consistency Posture

- Writes for control-plane metadata prioritize correctness and idempotency.
- Operational facts are append-first, then compacted into bounded-staleness views.
- Read paths are designed for stable contracts and predictable eventual convergence.

That same boundary applies to tenant-visible system tables: catalog reads remain
pointer-first, `/api/v1/query` is the initial SQL surface, and `system.*`
relations are read-only projections over published artifacts rather than a new
commit point.

Current scope note:

- Arco has strong repo-side proof for immutable snapshot publication, fenced head movement, pointer-first reads, and compactor-owned materialization.
- Broader governance domains such as grants, permissions, and credentials are not yet uniformly implemented on that same authoritative path.
- ADR-037, ADR-038, and ADR-039 define the catalog product, threat, and
  consistency contracts for those future domains.

## Canonical Decision Sources

For architectural decisions, use ADRs in `docs/adr/` as source of truth, especially:

- `docs/adr/adr-003-manifest-domains.md`
- `docs/adr/adr-005-storage-layout.md`
- `docs/adr/adr-018-tier1-write-path.md`
- `docs/adr/adr-020-orchestration-domain.md`
