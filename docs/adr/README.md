# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for Arco.

## Format

Each ADR follows this structure:
- **Status**: Proposed, Accepted, Deprecated, Superseded
- **Context**: What is the issue?
- **Decision**: What was decided?
- **Consequences**: What are the trade-offs?

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [001](adr-001-parquet-metadata.md) | Parquet-first metadata storage | Accepted |
| [002](adr-002-id-strategy.md) | ID strategy by entity type | Accepted |
| [003](adr-003-manifest-domains.md) | Manifest domain names and contention | Accepted |
| [004](adr-004-event-envelope.md) | Event envelope format and evolution | Accepted |
| [005](adr-005-storage-layout.md) | Canonical storage layout | Accepted |
| [006](adr-006-schema-evolution.md) | Parquet schema evolution policy | Accepted |
| [010](adr-010-canonical-json.md) | Canonical JSON serialization | Accepted |
| [011](adr-011-partition-identity.md) | Canonical partition identity | Accepted |
| [012](adr-012-asset-key-format.md) | AssetKey canonical string format | Accepted |
| [013](adr-013-id-wire-formats.md) | ID type wire formats | Accepted |
| [014](adr-014-leader-election.md) | Leader election strategy | Accepted |
| [015](adr-015-postgres-store.md) | Postgres orchestration store | Proposed |
| [016](adr-016-tenant-quotas.md) | Tenant quotas and fairness | Accepted |
| [017](adr-017-cloud-tasks-dispatcher.md) | Cloud Tasks dispatcher | Accepted |
