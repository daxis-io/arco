# Architecture

Arco is a file-native metadata platform with two tightly integrated domains:

- Catalog: metadata registry, lineage views, contracts, and search indexes.
- Orchestration: deterministic planning, execution state, and replay-safe event flows.

## Core Principles

- Metadata as immutable files on object storage.
- Query-native reads (DataFusion and browser-friendly read paths).
- Deterministic planning and replayable execution history.
- Tenant isolation at storage path, API, and policy boundaries.

## Domain Model

- Tier 1 metadata (low-frequency, strongly consistent): schema and control-plane state.
- Tier 2 metadata (high-frequency, append-first): execution and operational events.
- Compaction materializes query-efficient snapshots from append-first logs.

## Consistency Posture

- Writes for control-plane metadata prioritize correctness and idempotency.
- Operational facts are append-first, then compacted into bounded-staleness views.
- Read paths are designed for stable contracts and predictable eventual convergence.

## Canonical Decision Sources

For architectural decisions, use ADRs in `docs/adr/` as source of truth, especially:

- `docs/adr/adr-003-manifest-domains.md`
- `docs/adr/adr-005-storage-layout.md`
- `docs/adr/adr-018-tier1-write-path.md`
- `docs/adr/adr-020-orchestration-domain.md`
