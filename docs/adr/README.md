# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for Arco.

Design documents under `docs/plans/` are internal and may contain illustrative
pseudocode. ADRs are the source of record for architectural decisions.

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
| [015](adr-015-postgres-store.md) | Postgres orchestration store | Superseded |
| [016](adr-016-tenant-quotas.md) | Tenant quotas and fairness | Accepted |
| [017](adr-017-cloud-tasks-dispatcher.md) | Cloud Tasks dispatcher | Accepted |
| [018](adr-018-tier1-write-path.md) | Tier-1 write path architecture | Accepted |
| [019](adr-019-existence-privacy.md) | Existence privacy | Accepted |
| [020](adr-020-orchestration-domain.md) | Orchestration as unified domain | Accepted |
| [021](adr-021-cloud-tasks-naming.md) | Cloud Tasks naming convention | Accepted |
| [022](adr-022-dependency-satisfaction.md) | Per-edge dependency satisfaction | Accepted |
| [023](adr-023-worker-contract.md) | Worker contract specification | Accepted |
| [024](adr-024-schedule-sensor-automation.md) | Schedule and sensor automation | Accepted |
| [025](adr-025-backfill-controller.md) | Backfill controller | Accepted |
| [026](adr-026-partition-status-tracking.md) | Partition status tracking | Accepted |
| [027](adr-027-datafusion-query-endpoint.md) | DataFusion query endpoint | Accepted |
| [028](adr-028-gcp-oidc-authentication.md) | GCP OIDC authentication | Accepted |
| [029](adr-029-iceberg-multi-table-transactions.md) | Iceberg multi-table transactions (ICE-7) | Accepted |
| [030](adr-030-delta-uc-metastore.md) | Delta Lake + UC-Like Metastore/Catalog (Arco-Native) | Proposed |
| [031](adr-031-unity-catalog-api-facade.md) | Unity Catalog OSS API Facade (UC Parity) | Proposed |
| [032](adr-032-engine-boundaries.md) | Engine Boundaries and Split-Service Topology | Accepted |
