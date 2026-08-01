# Lineage L0 Golden Arrow Schema Plan

**Date:** 2026-07-30

**Status:** Planning artifact for lane L0 of the lineage program (ADR-042,
`docs/plans/2026-06-26-lineage-observation-projection-design.md`). This plan
defines target schemas and evolution rules. It does NOT change the live golden
schema for `lineage_edges.parquet`; implementation slices (L1+) evolve schemas
additively under ADR-006 and regenerate goldens through the existing
`generate_golden_schemas` flow in `crates/arco-catalog/tests/schema_contracts.rs`.

## Current Baseline

`system.lineage.edges` is projected from `lineage_edges.parquet` with exactly
six columns (`crates/arco-catalog/src/parquet_util.rs`, `LineageEdgeRecord`):

| Column | Type | Notes |
|---|---|---|
| `id` | utf8, non-null | Edge id. Historically a per-request ULID; as of the L0 route change, a content-derived sha256 hex id (see "Legacy Edge Identity" below). |
| `source_id` | utf8, non-null | Client-asserted upstream entity id. |
| `target_id` | utf8, non-null | Client-asserted downstream entity id. |
| `edge_type` | utf8, non-null | e.g. `derives_from`, `copies`, `transforms`. |
| `run_id` | utf8, nullable | Optional producing run. |
| `created_at` | int64 (ms), non-null | First-observation timestamp (fold is first-write-wins by id). |

The fold (`apply_lineage_event` in `tier1_compactor.rs`) dedupes by `id`,
first write wins. Edges are unvalidated client assertions with no existence or
ambiguity checks — structurally what the L2 gate forbids; acceptable as legacy
until L1/L2 land, and now bounded by route validation.

## Target: `system.lineage.observations`

Normalized diagnostic surface for observed events (design doc "Observation
Model"). All names snake_case; Arrow types shown; nullable unless stated.

| Column | Type | Non-null | Purpose |
|---|---|---|---|
| `observation_id` | utf8 | yes | Stable id; recommended `hash(tenant_id, producer, producer_event_id)` or `hash(tenant_id, producer, run_id, event_type, source_cursor)`. |
| `schema_version` | int32 | yes | Additive-evolution version stamp. |
| `tenant_id` | utf8 | yes | Logical scope (physical layout may already partition). |
| `workspace_id` | utf8 | yes | Logical scope. |
| `metastore_id` | utf8 | yes | Logical scope. |
| `run_id` | utf8 | no | Orchestration correlation. |
| `task_id` | utf8 | no | Orchestration correlation. |
| `job_id` | utf8 | no | Producer job identity (alias, not graph key). |
| `producer` | utf8 | yes | Producing component identity. |
| `producer_event_id` | utf8 | no | Producer's durable event id when available. |
| `producer_event_sequence` | int64 | no | Producer ordering hint. |
| `extractor_name` | utf8 | no | Extractor identity. |
| `extractor_version` | utf8 | no | Extractor version. |
| `event_type` | utf8 | yes | Observation kind. |
| `event_time` | timestamp(us, UTC) | no | Producer event time. |
| `ingested_at` | timestamp(us, UTC) | yes | Arco ingestion time. |
| `ingest_sequence` | int64 | yes | Arco ingestion order. |
| `source_cursor` | utf8 | no | Producer source order for deterministic replay. |
| `idempotency_key` | utf8 | no | Dedup key when producer supplies one. |
| `supersedes_observation_id` | utf8 | no | Correction chain. |
| `correction_reason` | utf8 | no | Stable enum-like string. |
| `input_dataset_count` | int32 | no | Envelope summary. |
| `output_dataset_count` | int32 | no | Envelope summary. |
| `has_column_lineage` | bool | no | Envelope summary. |
| `soundness_status` | utf8 | yes | Enum per ADR-042 rule 5. |
| `resolution_status` | utf8 | yes | Enum per ADR-042 rule 4. |
| `projection_watermark` | utf8 | no | Projection freshness cut. |
| `raw_observation_path` | utf8 | no | Internal/redacted raw payload reference. |
| `raw_observation_sha256` | utf8 | no | Raw payload checksum. |
| `raw_facet_summary_json` | utf8 | no | Redacted summary, never raw SQL/secrets. |
| `unresolved_inputs_json` | utf8 | no | Diagnostics. |
| `unresolved_outputs_json` | utf8 | no | Diagnostics. |
| `diagnostics_json` | utf8 | no | Machine-readable reasons, redacted. |

This table is a redacted diagnostic surface, not a raw event dump; raw
payloads live behind internal paths (design doc "Observation Model" and
"Access Control And Redaction").

## Target: `system.lineage.edges` (evolved, additive)

The compatibility table keeps its six columns unchanged and gains nullable
columns only (ADR-006). Planned additions, from the design doc's candidate
table-level edge fields:

- Scope: `tenant_id`, `workspace_id`, `metastore_id` (logical carry).
- Identity: `edge_key` (deterministic logical-dependency identity),
  `edge_instance_id` (one observed instance), `observation_id`,
  `raw_observation_ref`.
- Endpoint identity: `source_object_id`, `source_object_generation_id`,
  `source_schema_version_id`, `source_storage_snapshot_id`,
  `source_storage_snapshot_kind`, `source_materialization_id`,
  `source_materialization_time`, and the `target_*` mirrors. Object
  generation, schema version, storage snapshot, and materialization stay
  separate nullable fields — never one overloaded version column.
- Correlation: `job_id`, `task_id`, `event_time`, `catalog_snapshot_token`,
  `projection_watermark`.
- Quality: `extractor_name`, `extractor_version`, `soundness_status`,
  `resolution_status`. No numeric `confidence` field — enumerated statuses
  only (design doc "Identity Model").

Legacy mapping: existing rows read as `edge_key = id`,
`edge_instance_id = id`, endpoints resolved later by the L2 resolver
(`resolution_status` backfills as `unresolved_external` until then; legacy
rows are never silently upgraded to `resolved`).

Other L1+ tables (`identity_resolution`, `column_edges`, `materializations`,
`external_datasets`, `projection_watermarks`, governance
`asserted_tags`/`inferred_tag_reachability`) follow the design doc's candidate
field lists and get golden schemas when first implemented.

## Evolution Rules

1. Additive-only: new columns are nullable; existing columns never change
   type, name, or nullability (ADR-006; enforced by the
   `contract_*_backward_compatible` tests).
2. Every new lineage projection lands with a golden schema fixture and a
   backward-compatibility contract test in the same slice.
3. Enum-like columns (`soundness_status`, `resolution_status`,
   `edge_type`, `correction_reason`) are utf8 with documented closed
   vocabularies in ADR-042; adding a vocabulary value is additive, changing
   one's meaning is breaking.
4. `schema_version` stamps let projections coexist across additive versions;
   readers must tolerate unknown nullable columns.

## L0 Producer Contract: Worker Materialization Correlation

The embryonic producer contract already in the code is formalized as the L0
worker/materialization producer surface:

- `arco-worker-contract` carries delta correlation on task completion
  (`TaskOutput`, `crates/arco-worker-contract/src/lib.rs:386-394`): delta
  table, delta version, and partition "for lineage correlation".
- The orchestration fold materializes these into
  `CatalogRunIndexRow.delta_table` / `delta_version` / `delta_partition` /
  `execution_lineage_ref` (`crates/arco-flow/src/orchestration/compactor/fold.rs:1063-1072`).

Contract statement: workers report the storage-version identity of what they
actually materialized; the fold records it against the run/task; the future L3
edge projector consumes it as `*_storage_snapshot_id` /
`*_materialization_id` evidence with `resolved_by_materialization` status.
Producers must not fabricate versions they did not observe.

Known caveat, stated precisely because the two partition fields are easy to
conflate: the *reported* partition is landed, the *dispatched* one is not.
`TaskOutput.delta_partition` — what a worker reports it materialized — exists
on this branch and on `origin/main` (`c3c0867`) at
`crates/arco-worker-contract/src/lib.rs:394`, and folds into
`CatalogRunIndexRow.delta_partition`
(`crates/arco-flow/src/orchestration/compactor/fold.rs:1069`). L3 may consume
it as materialization evidence today; that lane is not blocked.

Issue #339 concerns the opposite direction. `WorkerDispatchEnvelope`
(`crates/arco-worker-contract/src/lib.rs:105-140`) carries no partition field
on this branch or on `origin/main`, so the control plane never tells a worker
which partition scope to execute against. The reported partition is therefore
self-asserted and unchecked against a dispatched scope, which is what would
corrupt materialization-identity fidelity for partitioned assets. A change
adding an additive `partition_key` to the dispatch envelope is in flight on a
parallel branch (`fix/runtime-convergence`, commit `6916890f`) and has not
landed. Until it does, L3 must not treat a reported partition as evidence that
the control plane scoped the work to that partition.

## Legacy Edge Identity (L0 route change)

As of this slice, `POST /api/v1/lineage/edges` derives each edge id
deterministically: `sha256("arco-lineage-edge-v1", source_id, target_id,
edge_type, run_id?)`, hex-encoded. Each field is absorbed length-prefixed
(byte length as a big-endian `u64`, then the bytes) rather than separated by a
delimiter, so the encoding is injective and no field content can shift the
boundary between two fields into another edge's digest; `run_id` carries a
presence tag so absent and empty stay distinct. Validation independently
rejects control characters in all four fields. Combined with the fold's first-write-wins
dedup by id, duplicate POSTs (client retries without an Idempotency-Key)
converge to a single projected row; `created_at` records the first accepted
observation. Tenant/workspace scoping is physical (scoped storage), so equal
content in different scopes cannot collide. This mechanism was chosen over
the heavyweight `claim_idempotency` transaction path because the edge event
is commutative and the fold already dedupes by id — content-derived identity
makes the route idempotent with zero new state.
