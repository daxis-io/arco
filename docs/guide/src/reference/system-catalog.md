# System Catalog

Arco exposes a logical `system` catalog through `/api/v1/query` for read-only
SQL access to operational metadata. These tables are tenant-visible projections
derived from pointer-published snapshots and runtime projections; immutable
ledger records and pointer-published manifests remain the source of truth.

## Tables

Catalog metadata:

- `system.catalog.catalogs`
- `system.catalog.namespaces`
- `system.catalog.tables`
- `system.catalog.columns`
- `system.catalog.commits`

Lineage metadata:

- `system.lineage.edges`

Orchestration metadata:

- `system.orchestration.runs`
- `system.orchestration.tasks`
- `system.orchestration.catalog_run_index`
- `system.orchestration.dep_satisfaction`
- `system.orchestration.timers`
- `system.orchestration.dispatch_outbox`
- `system.orchestration.sensor_state`
- `system.orchestration.sensor_evals`
- `system.orchestration.partition_status`
- `system.orchestration.schedule_definitions`
- `system.orchestration.schedule_state`
- `system.orchestration.schedule_ticks`
- `system.orchestration.backfills`
- `system.orchestration.backfill_chunks`
- `system.orchestration.run_key_conflicts`

## Planned Expansion

The current registered surface intentionally stops at catalog, lineage, and
orchestration projections. The planned list below is non-exhaustive. These
tables are not registered until Arco has authoritative native state and safe
Parquet projections for each domain:

- `system.access.{grants,compiled_permissions,audit,auth_denies,credential_mints}`
- `system.storage.{credentials,external_locations,managed_roots,workspace_bindings}`
- `system.catalog.{volumes,functions,registered_models,model_versions}`
- `system.governance.attachments`
- `system.delta.{tables,commits,staged_commits,reconciliation_issues}`

## Examples

```sql
select name
from system.catalog.tables
order by name;
```

```sql
select commit_ulid, snapshot_version
from system.catalog.commits
order by published_at desc;
```

```sql
select run_id, state
from system.orchestration.runs
order by created_at desc;
```

```sql
select org_id, workspace_id, run_id, task_key, asset_key
from system.orchestration.catalog_run_index
order by updated_at desc;
```

```sql
select asset_key, stale_reason_code
from system.orchestration.partition_status
order by asset_key;
```

```sql
select backfill_id, state
from system.orchestration.backfills
order by created_at desc;
```

## Freshness And Boundaries

System tables are scoped to the request tenant and workspace. They reflect the
currently visible pointer-published snapshots and may lag raw event ingestion
until the relevant projection has been compacted and published.

The `system` catalog is not a synchronous authorization or correctness path.
Do not use it as the commit point for control-plane decisions. Raw ledger
paths, raw manifest JSON, raw search postings, and internal orchestration
dedupe/index tables such as `run_key_index` and `idempotency_keys` are not part
of the default tenant-visible surface.
