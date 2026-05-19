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

## Deferred Tables

The initial surface does not include access, storage, governance, volume,
function, model registry, or managed Delta operational tables. Those tables
remain planned until their authoritative native state and safe Parquet
projections exist:

- `system.access.{grants,compiled_permissions,audit,auth_denies,credential_mints}`
- `system.storage.{credentials,external_locations,managed_roots,workspace_bindings}`
- `system.catalog.{volumes,functions,registered_models,model_versions}`
- `system.governance.attachments`
- `system.delta.{tables,commits,staged_commits,reconciliation_issues}`

## Boundaries

- Truth: immutable ledger + pointer-published manifests
- Serving path: manifest-selected Parquet artifacts
- Operations surface: read-only system tables
- Not exposed by default: raw ledger paths, raw manifest JSON, raw search
  postings, and internal orchestration dedupe/index tables such as
  `run_key_index` and `idempotency_keys`

## Consequences

- `/api/v1/query` becomes the initial SQL surface for tenant-visible system
  tables.
- The system-table surface is explicit and allowlisted rather than inferred from
  every file in a manifest.
- System tables remain asynchronous projections and are not part of the
  synchronous correctness or authorization path.
