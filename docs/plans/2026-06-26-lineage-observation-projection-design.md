# Lineage Observation And Projection Design

**Date:** 2026-06-26

**Status:** Planning design. This document records the target architecture and
execution slices for future implementation. It does not approve route behavior,
schema changes, or storage-backend migration by itself.

## Purpose

Arco should treat lineage as append-only observed fact that is deterministically
projected into catalog, search, governance, and system-table views. Lineage
must not become a mutable field on catalog rows, and orchestrators must not
directly edit catalog objects to publish lineage.

This design turns the Headwaters/OpenLineage lessons into an Arco-specific
model that preserves:

- stable Arco catalog object identity;
- versioned table and materialization identity;
- replayable object-store projections;
- clear orchestration/catalog boundaries;
- soundness diagnostics instead of fabricated precision;
- asserted governance facts separate from inferred propagation.

## Prior Art And Local Fit

Headwaters is useful prior art because it makes two separations explicit:

- emit OpenLineage run events with jobs, runs, input/output datasets, and
  column lineage from the engine;
- ingest those events into an append-only log and project normalized read
  tables for graph, browse, tag, and compatibility APIs.

Useful references:

- Headwaters README:
  <https://github.com/open-lakehouse/headwaters/blob/main/README.md>
- Headwaters DataFusion/OpenLineage design:
  <https://github.com/open-lakehouse/headwaters/blob/main/docs/open-lineage-design.md>
- Headwaters hybrid-CQRS storage ADR:
  <https://github.com/open-lakehouse/headwaters/blob/main/docs/adr/0006-hybrid-cqrs-postgres-storage.md>
- Headwaters backend-agnostic mutation IR ADR:
  <https://github.com/open-lakehouse/headwaters/blob/main/docs/adr/0007-mutation-ir-projection-pipeline.md>
- Headwaters tag-as-fact and propagation ADRs:
  <https://github.com/open-lakehouse/headwaters/blob/main/docs/adr/0008-tags-as-discovered-facts.md>
  and
  <https://github.com/open-lakehouse/headwaters/blob/main/docs/adr/0009-tag-pii-propagation.md>

The Arco fit is strong because existing Arco plans already prefer immutable
mutations, typed Parquet projections, fenced pointer publication, read-only
system/discovery views, stable IDs as enforcement keys, and compatibility APIs
as adapters over Arco-owned state. The Tier-1 control-store strategy also gives
the right future direction: authoritative transactions may return state tokens,
while async compaction publishes Parquet projections and system tables.

## Design Rule

OpenLineage is Arco's external lineage exchange envelope, not Arco's internal
catalog storage model.

OpenLineage dataset `namespace` and `name` values are aliases and join hints.
They are not durable Arco graph keys. The projection layer resolves them to
Arco stable object identities where possible, preserves unresolved external
datasets where not, and records resolution status explicitly.

Arco's internal lineage graph keys are stable IDs plus version/materialization
identity. Names, paths, table references, and OpenLineage dataset identifiers
are evidence used by the resolver.

## Target Flow

```text
OpenLineage-shaped ingest event
  -> Arco lineage observation event in object storage or control log
  -> identity resolution against stable Arco catalog IDs
  -> deterministic projection
  -> system.lineage.* Parquet views
  -> catalog, discovery, search, and governance read views
```

The flow is intentionally compatible with both current object-store ledger plus
compactor architecture and a future Tier-1 control-store transaction model. The
projection contract is more important than the hot storage substrate.

## Replay, Dedupe, And Correction Semantics

Append-only observations need explicit retry, duplicate, late-event, and
correction behavior before implementation.

Observation envelopes should include:

```text
observation_id
observation_kind
tenant_id
workspace_id
metastore_id
producer
producer_event_id
producer_event_sequence
producer_run_attempt
source_cursor
ingest_sequence
idempotency_key
event_time
ingested_at
supersedes_observation_id
correction_reason
raw_observation_path
raw_observation_sha256
```

Recommended idempotency keys:

```text
hash(tenant_id, producer, producer_event_id)
```

or, when a producer has no durable event id:

```text
hash(tenant_id, producer, run_id, event_type, source_cursor)
```

The invariant is:

```text
Replaying the same observations in source-cursor order against the same
catalog identity snapshots must produce byte-equivalent projections.
```

`ingest_sequence` records Arco ingestion order. `source_cursor` records the
producer's source order. The projector should process deterministic source
order where available and retain ingestion order for diagnostics. Duplicate
observations should be idempotent. Late observations should either project into
the correct historical version/materialization context or remain diagnostic
only if their resolver context is unavailable. Corrections should append a new
observation with `supersedes_observation_id` and a stable
`correction_reason`; they should not mutate or delete the original observation.

## Event Families

Keep the event families separate even when they join through shared IDs.

| Family | Examples | Source of truth | Projection examples |
|---|---|---|---|
| Orchestration events | task lifecycle, schedules, retries, heartbeats, callbacks | orchestration ledger/fold | `system.orchestration.*`, `catalog_run_index` |
| Lineage observation events | run/job/dataset/column observations, engine-emitted OpenLineage facts | lineage observation log/control stream | `system.lineage.*`, search lineage facets |
| Catalog/governance events | object creation, rename, grants, tags, policy attachments | catalog/governance ledger/control store | `system.catalog.*`, `system.access.*`, `system.governance.*` |

These families can join by `run_id`, `task_id`, `job_id`, stable object IDs,
and materialization IDs. They should not all share one hot path, one retention
contract, or one compaction cadence. In particular, high-frequency
orchestration telemetry and raw lineage observations should not become the
first strongly consistent Tier-1 control-store tranche.

## Identity Model

### Durable Object Identity

Arco graph edges should use stable catalog IDs:

- `table_id`, `column_id`, and future object-family IDs for Arco-managed
  catalog objects;
- generated external dataset IDs for datasets not resolved to Arco objects;
- stable job IDs where Arco owns the job/asset identity, with OpenLineage job
  namespace/name retained as aliases.

Names and paths remain mutable lookup attributes. They are never sufficient as
internal edge keys.

### Version And Materialization Identity

Lineage answers are ambiguous without version/materialization dimensions. Edges
should be able to distinguish:

- Delta table versions;
- Iceberg snapshot IDs;
- CREATE OR REPLACE table generations;
- schema evolution and column reuse;
- physical materialization windows;
- worker-published output visibility.

The minimum internal edge identity should include version/materialization
fields even if early projections leave some nullable.

Candidate table-level edge fields:

```text
edge_id
tenant_id
workspace_id
metastore_id
edge_key
edge_instance_id
source_object_id
source_object_generation_id
source_schema_version_id
source_storage_snapshot_id
source_storage_snapshot_kind
source_materialization_id
source_materialization_time
target_object_id
target_object_generation_id
target_schema_version_id
target_storage_snapshot_id
target_storage_snapshot_kind
target_materialization_id
target_materialization_time
run_id
job_id
task_id
edge_type
event_time
ingested_at
catalog_snapshot_token
projection_watermark
extractor_name
extractor_version
soundness_status
resolution_status
observation_id
raw_observation_ref
```

Candidate column-level edge fields:

```text
edge_id
tenant_id
workspace_id
metastore_id
edge_key
edge_instance_id
source_object_id
source_column_id
source_column_generation_id
source_object_generation_id
source_schema_version_id
source_storage_snapshot_id
source_storage_snapshot_kind
source_materialization_id
target_object_id
target_column_id
target_column_generation_id
target_object_generation_id
target_schema_version_id
target_storage_snapshot_id
target_storage_snapshot_kind
target_materialization_id
run_id
job_id
task_id
edge_type
transformation_kind
dependency_role
propagation_default
event_time
ingested_at
catalog_snapshot_token
projection_watermark
extractor_name
extractor_version
soundness_status
resolution_status
observation_id
raw_observation_ref
```

Avoid a generic numeric `confidence` field unless a producer has a defensible
confidence model. Most lineage extractors can report exactness, degradation,
or unresolved identity more honestly through enumerated statuses.

`edge_key` is the deterministic identity of the logical dependency.
`edge_instance_id` is the deterministic identity of one observed instance of
that dependency. The same source-to-target relationship may appear many times
across runs, retries, backfills, and incremental materializations, so both
identities are useful.

Object generation, schema version, storage snapshot, and materialization are
separate concepts:

- object generation distinguishes CREATE OR REPLACE and object replacement;
- schema version distinguishes schema evolution and column reuse;
- storage snapshot distinguishes Delta versions, Iceberg snapshots, object
  versions, and unknown storage-version kinds;
- materialization identifies a produced runtime output or visibility event.

Column lineage must either make column IDs generation-safe or carry explicit
`source_column_generation_id` and `target_column_generation_id`. Drop/re-add
column reuse must not collapse into a single historical column identity.

## Identity Resolution

The resolver maps observation aliases to Arco identities. It should be a
projector stage with its own diagnostics, not a hidden helper.

Inputs may include:

- OpenLineage dataset `namespace` and `name`;
- object-store URL, path, and symlink facets when available;
- catalog/schema/table aliases;
- table location and table format metadata;
- job namespace/name and parent run context;
- worker task payload and `catalog_run_index` metadata;
- run output metadata such as Delta version, output path, and materialization
  ID.

Resolution records and projected edges should include as-of metadata:

```text
resolver_version
resolution_policy_version
catalog_snapshot_token
catalog_projection_watermark
resolved_as_of_time
candidate_count
matched_evidence_json
candidate_ids_redacted_json
```

Do not resolve historical observations against whatever catalog name happens
to be current at read time. The resolver must record which catalog/governance
snapshot it used. Rebuilding projections later should use the recorded
resolution context or intentionally produce a new correction/diagnostic
observation rather than silently changing old edge identity.

Resolution statuses should be explicit:

| Status | Meaning |
|---|---|
| `resolved` | Alias maps to exactly one live Arco object/version. |
| `resolved_by_materialization` | Matched through a run output or materialization record. |
| `unresolved_external` | Dataset is outside Arco authority and gets an external dataset identity. |
| `ambiguous` | Alias/path matches multiple candidates. |
| `stale_alias` | Alias matched an old name that no longer points to the observed object. |
| `deleted_object` | Observation references an object that exists only in history. |
| `replaced_object` | Name/path now points to a replacement object generation. |
| `unsupported_format` | Dataset shape cannot be resolved under current format support. |

`ambiguous`, `stale_alias`, `deleted_object`, and `replaced_object` must not be
silently projected as exact graph edges. They should appear in diagnostics and,
where useful, in degraded table-level observations.

## Soundness Contract

Arco should copy Headwaters' soundness stance: unknown is safer than fabricated
precision.

For column lineage:

- publish column edges only when the extractor can resolve sources and targets
  soundly;
- if an unhandled plan node, arity mismatch, unresolvable column reference,
  expression subquery, unsupported extension node, or resolver ambiguity makes
  a column facet unsound, drop the whole column-lineage facet for that
  statement;
- keep table-level lineage when table-level input/output identity is sound;
- never fall back to name-only column matching to create precise-looking edges.

Recommended `soundness_status` values:

| Status | Meaning |
|---|---|
| `exact` | Edge is resolved under the extractor's soundness rules. |
| `table_only` | Table-level lineage is known, column lineage is absent or dropped. |
| `dropped_column_facet` | Column-lineage facet was rejected as a unit. |
| `partial_input_stats_only` | Runtime statistics exist but cannot be attributed per input. |
| `unsupported_plan_shape` | Extractor saw unsupported logical or physical plan shape. |
| `unresolved_reference` | One or more references could not be resolved. |
| `identity_ambiguous` | Dataset alias resolution was ambiguous. |
| `external_unresolved` | Dataset is external and intentionally not mapped to Arco object IDs. |

Projection code should prefer explicit diagnostic records over best-effort edge
fabrication.

## Dependency Roles

Column edges need dependency roles because governance propagation depends on
how a column influenced the output.

Recommended `dependency_role` values:

| Role | Meaning |
|---|---|
| `value` | Input value flows into the output value. |
| `predicate` | Input affects row inclusion through a filter predicate. |
| `join_key` | Input affects row matching or row presence through a join key. |
| `grouping_key` | Input groups rows but may or may not be emitted. |
| `aggregate_input` | Input contributes to an aggregate output value. |
| `window_partition` | Input partitions window calculations. |
| `window_order` | Input orders window calculations. |
| `sort_limit` | Input affects order or limit selection. |
| `control_dependency` | Input affects control flow without value flow. |

Recommended `propagation_default` values:

| Value | Meaning |
|---|---|
| `propagates_tag` | Tags such as PII normally propagate through this role. |
| `does_not_propagate_tag` | Tags normally do not propagate through this role. |
| `policy_dependent` | Propagation depends on tag type or policy. |

For example, a PII column used only as a join key may influence row presence
without producing a PII output value. A governance view should be able to say
"PII value flows here" separately from "PII influenced row selection here."

## Observation Model

Lineage observations should keep enough normalized shape for projections and
enough raw reference for audit/debugging.

Candidate `system.lineage.observations` fields:

```text
observation_id
schema_version
tenant_id
workspace_id
metastore_id
run_id
task_id
job_id
producer
extractor_name
extractor_version
event_type
event_time
ingested_at
input_dataset_count
output_dataset_count
has_column_lineage
soundness_status
resolution_status
projection_watermark
raw_observation_path
raw_observation_sha256
raw_facet_summary_json
unresolved_inputs_json
unresolved_outputs_json
diagnostics_json
```

This table is not a raw event dump. It is a redacted, normalized diagnostic
surface explaining what arrived and how projections interpreted it.

Raw event payloads, if retained, should live behind internal paths or redacted
debug access, not ordinary system-table scans.

## Projection Model

The lineage projection should be replayable from observation and catalog state.
It should not contain hand-mutated state that cannot be rebuilt.

Candidate system tables:

| Table | Purpose |
|---|---|
| `system.lineage.observations` | Normalized diagnostic surface for observed events. |
| `system.lineage.identity_resolution` | Alias/path/reference to Arco object resolution records. |
| `system.lineage.edges` | Table/object-level resolved lineage edges. |
| `system.lineage.column_edges` | Column-level resolved lineage edges. |
| `system.lineage.materializations` | Run/task output versions, materialization IDs, paths, and visibility. |
| `system.lineage.external_datasets` | Durable identities for unresolved external datasets. |
| `system.lineage.projection_watermarks` | Projection freshness, source cursors, and rebuild metadata. |
| `system.governance.asserted_tags` | Direct facts asserted by scanners, users, or producers. |
| `system.governance.inferred_tag_reachability` | Derived downstream propagation over lineage graph. |

Existing `system.lineage.edges` can remain the compatibility table-level graph
while richer projections are added. Compatibility adapters may reshape these
views for Marquez/OpenLineage-adjacent consumers, but the native Arco views own
the semantics.

Every projected table must carry `tenant_id`, `workspace_id`, and
`metastore_id` logically, even when the physical layout is already partitioned
by scope. The fields make exports, debug bundles, redaction checks, and
cross-tenant safety tests self-describing.

## Governance Facts And Propagation

Separate asserted facts from inferred facts.

Asserted fact example:

```text
scanner observed tag=pii on table_id=T, column_id=C at event_time=E
```

Derived propagation example:

```text
tag=pii reaches table_id=T2, column_id=C2 through graph G
at projection_watermark=W
```

Rules:

- scanners, classifiers, human tools, and engines append governance assertion
  events;
- projections materialize current asserted tag assignments;
- propagation is derived by traversing table or column lineage edges;
- inferred propagation must carry `inferred=true`, source tag assignment IDs,
  graph/projection watermark, dependency roles used, and derivation metadata;
- do not materialize propagated tags as if they were hand-authored or
  scanner-authored assignments.

Column-level propagation should be preferred when column edges are exact.
Table-level fallback is allowed only when the view names that degradation.

## Storage And Projection Architecture

Arco should imitate Headwaters' storage shape, not its Postgres dependency:

```text
append-only observation/control stream
  -> pure facet and observation processors
  -> backend-neutral mutation/projection IR
  -> Parquet projection applier
  -> pointer-published system tables and read views
```

The projection processor should be pure: `observation + current catalog
identity snapshot identified by catalog_snapshot_token -> mutations`. The
applier owns idempotency, latest-wins, correction, and watermark guards.

This keeps three future paths open:

- current object-store ledger plus synchronous or async compactor;
- future Arco control-store transaction log plus async Parquet projection;
- test-only in-memory applier for fast projection equivalence tests.

Projection publication must keep the existing Arco rule: system tables are
derived, watermarked, redacted, and never enforcement inputs.

## Access Control And Redaction

Lineage can expose sensitive information: paths, table names, column names,
SQL text, tags, ownership hints, errors, and job metadata.

Minimum policy:

- ordinary system-table readers see only objects they are authorized to inspect;
- raw SQL, raw facets, tokens, credentials, private paths, and detailed errors
  are redacted unless an admin/debug scope allows them;
- external dataset identities avoid leaking raw object-store URLs when not
  authorized;
- inferred governance propagation views follow the stricter of source and
  target visibility rules;
- diagnostics expose machine-readable reasons without exposing secrets.

Authorization should use authoritative catalog/governance state or compiled
views, not lineage system-table output.

Partially visible graph behavior should be explicit:

| View | Behavior |
|---|---|
| Default user view | Show only edges where both endpoints are visible. |
| Debug/admin lineage view | Show redacted hidden endpoints with reason codes. |
| Governance/security view | May show hidden upstream/downstream reachability under stricter policy. |

The default user view should not leak hidden object names, paths, tags, edge
counts, or neighborhood shape through lineage graphs, search facets, or
diagnostics. Hidden endpoint handling belongs in the projection/read policy,
not in ad hoc UI filtering.

## Execution Slices

### Slice 0: Contract And Schema Design

Define native Arco lineage contracts before implementation:

- observation envelope;
- replay, dedupe, and correction semantics;
- identity-resolution statuses;
- soundness statuses;
- edge schemas with version/materialization fields;
- resolver as-of semantics;
- asserted vs inferred governance schema;
- retention and redaction policy.

Expected outputs:

- ADR or guide page for lineage observation/projection architecture;
- golden Arrow schema plan for `system.lineage.*`;
- compatibility note explaining OpenLineage as envelope, not storage model.

### Slice 1: Normalized Observations

Add an append-only lineage observation event family and a redacted normalized
observation projection.

Acceptance criteria:

- raw observation can be replayed;
- `system.lineage.observations` explains event identity, producer, extractor,
  timestamps, counts, soundness, and raw reference;
- duplicate/retry observations are idempotent;
- late observations produce deterministic projections or explicit diagnostics;
- correction observations supersede prior observations without mutating them;
- malformed or unsupported observations produce diagnostics, not edges.

### Slice 2: Identity Resolution Projection

Implement resolver diagnostics before relying on resolved graph edges.

Acceptance criteria:

- OpenLineage namespace/name/path aliases resolve to stable Arco IDs when
  exact;
- resolution records include resolver version, policy version,
  catalog snapshot token, candidate count, and matched evidence;
- unresolved external datasets receive stable external identities;
- ambiguous, stale, deleted, and replaced references are visible in
  `system.lineage.identity_resolution`;
- no graph edge silently treats ambiguous identity as exact.

### Slice 3: Versioned Table Edges

Evolve table-level `system.lineage.edges` to include version/materialization
identity and observation references.

Acceptance criteria:

- Delta version, Iceberg snapshot, materialization ID, and output visibility
  fields are represented where known;
- object generation, schema version, storage snapshot, and materialization are
  separate nullable fields rather than one overloaded version column;
- table rename and replace tests prove stable IDs beat mutable names;
- historical observations are not resolved against current catalog names at
  read time;
- replay is deterministic and idempotent.

### Slice 4: Column Edges With Soundness Guards

Add column-level projection only after exact resolver/extractor semantics are
available.

Acceptance criteria:

- exact column lineage projects into `system.lineage.column_edges`;
- column edges carry dependency roles and propagation defaults;
- unsupported plan or ambiguous column resolution drops the whole column facet;
- table-level edges remain when table-level lineage is sound;
- tests prove there is no name-only fallback.

### Slice 5: Governance Assertions And Propagation

Add governance fact events and derived propagation views.

Acceptance criteria:

- asserted scanner/user tags are replayable events;
- inferred downstream propagation is marked inferred and watermarked;
- inferred propagation records source tag assignment IDs and dependency roles
  used;
- column-level propagation wins over table-level fallback;
- rebuild from event log reproduces both assertion and propagation views.

### Slice 6: Compatibility And Search Surfaces

Expose compatibility and product views without making them authoritative.

Acceptance criteria:

- Marquez/OpenLineage-style adapters consume native projections;
- search indexes include lineage facets and diagnostics as derived postings;
- system-table docs state freshness and redaction behavior;
- compatibility APIs cannot mutate catalog rows to write lineage.

## Testing Strategy

Tests should prove invariants rather than only happy-path output.

Required categories:

- schema golden tests for every new Parquet projection;
- replay/idempotency tests for observations and projections;
- duplicate/retry observation tests;
- late and out-of-order observation tests;
- resolver tests for exact, unresolved external, ambiguous, stale alias,
  deleted object, and replaced object cases;
- catalog-snapshot replay tests proving old observations do not resolve
  against current names;
- table rename and CREATE OR REPLACE lineage tests;
- Delta version and Iceberg snapshot identity tests;
- soundness tests proving unsupported column lineage is dropped as a unit;
- redaction tests for SQL/facet/path/tag diagnostics;
- hidden-endpoint redaction and leakage tests for partially visible graphs;
- propagation tests separating asserted and inferred governance facts;
- projection-watermark tests for stale system-table behavior;
- compatibility tests proving OpenLineage names are aliases, not graph keys.

## Non-Goals

- Do not make OpenLineage the internal Arco catalog model.
- Do not make Marquez compatibility the product goal.
- Do not let orchestrators mutate catalog rows directly.
- Do not store inferred governance propagation as asserted tag assignments.
- Do not put raw high-volume orchestration telemetry into the first Tier-1
  control-store tranche.
- Do not use system tables, search indexes, or lineage projections as
  enforcement inputs.
- Do not emit fabricated column edges through name-only fallback.
- Do not resolve historical observations against whatever catalog name happens
  to be current at read time.

## Open Questions

- Which service first owns OpenLineage-shaped ingestion: `arco-api`,
  `arco-flow`, or a dedicated lineage service boundary?
- How long should raw observation payloads be retained after normalized
  observations and projections are published?
- Should external dataset IDs be scoped by tenant, metastore, or global
  namespace?
- What is the first producer: Arco workers, DataFusion query routes, Delta
  commit callbacks, or a scanner/governance process?
- How should root-token reads interact with lineage projections that lag the
  catalog object snapshot?
- Which redaction tier can inspect raw SQL and raw facet payloads?
- Should inferred tag propagation be query-time only at first, or materialized
  with explicit watermarks from day one?

## Recommended Initial Choices

These choices should guide the first implementation plan unless a prototype
finds a concrete blocker:

- Ingestion boundary: start in `arco-api` or a small lineage-ingest module
  behind `arco-api`. `arco-flow` and workers emit observations but do not own
  lineage semantics. A dedicated lineage service boundary can wait until event
  volume or external producer needs justify it.
- First producer: start with Arco worker/materialization publishing rather
  than DataFusion column extraction. Materialization observations have stable
  object IDs, run/task IDs, output paths, storage versions, and visibility with
  lower soundness risk.
- External dataset IDs: scope by at least
  `(tenant_id, workspace_id, metastore_id, normalized_namespace,
  normalized_name_or_path_hash)`. Do not use global external dataset identity
  until cross-tenant isolation and leakage risks are formally handled.
- Root-token reads: return lineage as of its projection watermark and catalog
  snapshot token. Do not repair projection lag by resolving against live
  catalog state in the read path.
- Inferred tag propagation: start query-time or admin-only until edge roles and
  dependency semantics are stable. Materialize later with explicit
  `projection_watermark`, `graph_version`, source tag assignment IDs,
  dependency roles used, and `inferred=true`.

## Decision Summary

Arco should treat lineage observations as durable facts and publish lineage as
derived, replayable views. OpenLineage provides the exchange shape. Arco owns
identity, version/materialization semantics, soundness policy, governance
meaning, access control, watermarks, and projection behavior.

The immediate planning target is not "add OpenLineage" or "add Marquez." It is
to evolve the current table-level lineage graph into a native Arco lineage
projection family: observations, identity resolution, versioned table edges,
column edges, materializations, diagnostics, and asserted-vs-inferred
governance views.
