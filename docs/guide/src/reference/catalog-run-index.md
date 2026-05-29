# Catalog Run Index

`catalog_run_index` is an orchestration compactor projection for catalog and
lineage readers that need run-backed asset metadata without listing every
canonical run object. It is published as a Parquet table artifact through the
same immutable orchestration manifest pointer as `runs.parquet` and
`tasks.parquet`. The manifest records these artifacts in
`catalog_run_index_by_org`, keyed by `org_id`, so a reader for one organization
can perform a known-key manifest lookup and read only that organization's index
files.

The projection is not canonical state. The orchestration event ledger and the
folded `runs` and `tasks` projections remain the source of truth. The index can
always be rebuilt by replaying the ledger through the compactor fold.

## Scope

Each row is keyed by:

| Column | Meaning |
| --- | --- |
| `org_id` | Organization scope for catalog readers. In the current Arco-native contract this is the orchestration event `tenant_id`. |
| `workspace_id` | Workspace scope from the orchestration event envelope. |
| `run_id` | Authoritative orchestration run identifier. |
| `task_key` | Task within the run that targets a catalog asset. |

Only tasks with an `asset_key` are included. Non-asset orchestration work remains
visible through the regular orchestration projections.

## Reader Contract

Catalog readers should resolve the current orchestration manifest pointer, read
the manifest-selected base snapshot and visible L0 deltas, and consume the
`catalog_run_index_by_org[org_id]` table artifact when present. They should not
list legacy flat prefixes such as `authoritative-runs/runs/` or
`shadow-runs/runs/` to discover current run metadata.

Readers that combine base and L0 artifacts must merge by
`(org_id, workspace_id, run_id, task_key)` and keep the row with the greatest
`row_version`. This matches the compactor merge rule and prevents stale attempt,
heartbeat, or visibility events from regressing catalog metadata.

The query API exposes the current workspace's base-snapshot org artifact as
`system.orchestration.catalog_run_index` when that artifact has been merged into
the visible base snapshot. Readers that need visible L0 freshness should use the
manifest contract directly and merge base plus L0 artifacts.

The projection is physically split by `org_id` at the manifest artifact level.
That removes both the bucket-wide canonical run-object listing path and the need
to read other organizations' catalog-index rows during a cold refresh.

## Columns

The required identity and ordering columns are:

| Column | Required | Notes |
| --- | --- | --- |
| `schema_version` | yes | Projection schema version. |
| `org_id` | yes | Maps to `tenant_id` until Arco introduces a separate org identifier. |
| `workspace_id` | yes | Workspace from the event envelope. |
| `run_id` | yes | Run identifier. |
| `task_key` | yes | Task identifier within the run. |
| `plan_id` | yes | Run plan identifier. |
| `run_status` | yes | Current folded run status. |
| `cancel_requested` | yes | True when cancellation has been requested and the run is not necessarily terminal yet. |
| `task_status` | yes | Current folded task status. |
| `attempt` | yes | Current task attempt. |
| `triggered_at` | yes | Run trigger timestamp. |
| `updated_at` | yes | Latest meaningful derived update timestamp for this row. |
| `row_version` | yes | Greatest source run/task row version used to derive the row. |

Additional catalog, output, lineage, and error columns are nullable unless their
source field is always present in folded state. Nullable fields include
`run_key`, `kind`, `reference_id`, `source_type`, `asset_key`,
`target_namespace`, `target_table`, `partition_key`, `attempt_id`,
`materialization_id`, `output_visibility_state`, `published_at`,
`publish_error`, `delta_table`, `delta_version`, `delta_partition`,
`execution_lineage_ref`, `started_at`, `last_heartbeat_at`, `completed_at`,
`code_version`, and `error_message`.

## Lifecycle

The fold derives rows after run and task lifecycle events. `PlanCreated` creates
rows for asset tasks. `DispatchRequested`, `TaskStarted`, `TaskHeartbeat`,
`TaskFinished`, and `TaskOutputVisibilityChanged` update only affected task rows
unless a run-level field changes. Run cancellation or completion updates the
rows for that run because cancellation, `run_status`, and completion metadata
are part of the projection.

Stale attempts and stale output visibility events follow the existing task fold
guards. If the canonical `tasks` row is not updated, the catalog index row is
not updated.

## Backfill

Existing data under legacy `authoritative-runs/runs/` or `shadow-runs/runs/`
prefixes needs a one-time migration into the Arco-native ledger/projection
boundary. The safe migration path is to replay or synthesize fold-consistent
orchestration events, run the compactor, and publish the derived projection.

Do not keep a long-lived dual-read path from catalog readers to both the legacy
flat run prefixes and `catalog_run_index`. That would reintroduce ambiguous
authority and make lifecycle correctness depend on two independent sources.

## Schema Evolution

The schema is covered by a golden Arrow schema fixture. Compatible changes must
be additive and nullable unless the projection schema version is intentionally
bumped with a migration plan. Required identity or ordering columns are part of
the merge contract and must not be removed or retyped without an explicit
breaking-version transition.
