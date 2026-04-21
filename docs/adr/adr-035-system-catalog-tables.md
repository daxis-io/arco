# ADR-035: System Catalog Tables

## Status

Proposed

## Context

Arco already serves manifest-selected Parquet snapshots through pointer-first
read paths. Operators and tenants also need a stable SQL surface for the
current operational state that matters for debugging, governance, scheduling,
backfills, and run operations, without promoting raw manifests, ledger files,
or internal search postings to the default user-facing contract.

The serving surface needs to remain derived and read-only. The commit point for
catalog and orchestration state remains immutable artifact publication plus
pointer movement, not the SQL-facing tables exposed later by the API.

## Decision

Arco exposes a tenant-visible logical `system` catalog that contains read-only
operational tables derived from pointer-published snapshots and runtime
projections.

## Initial Tables

- `system.catalog.{catalogs,namespaces,tables,columns,commits}`
- `system.lineage.edges`
- `system.orchestration.{runs,tasks,dep_satisfaction,timers,dispatch_outbox,sensor_state,sensor_evals,partition_status,schedule_definitions,schedule_state,schedule_ticks,backfills,backfill_chunks,run_key_conflicts}`

## Boundaries

- Truth: immutable ledger + pointer-published manifests
- Serving path: manifest-selected Parquet artifacts
- Operations surface: read-only system tables
- Not exposed by default: raw ledger, raw manifests, raw search postings

## Consequences

- `/api/v1/query` becomes the initial SQL surface for tenant-visible system
  tables.
- The system-table surface is explicit and allowlisted rather than inferred from
  every file in a manifest.
- System tables remain asynchronous projections and are not part of the
  synchronous correctness or authorization path.
