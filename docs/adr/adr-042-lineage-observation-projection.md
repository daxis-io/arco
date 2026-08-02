# ADR-042: Lineage Observation And Projection Architecture

## Status

Proposed

## Context

Arco's current lineage surface is a single table-level edge store: clients POST
`/api/v1/lineage/edges`, the API appends `LineageDdlEvent::EdgesAdded` events
under the lineage Tier-1 domain, and sync compaction folds them into
`lineage_edges.parquet`, exposed as `system.lineage.edges`. Edges are
client-asserted, six fields wide, and carry no version, materialization,
resolution, or soundness identity.

The [lineage observation and projection design](../plans/2026-06-26-lineage-observation-projection-design.md) defines the
target architecture (lanes L0–L6): lineage as append-only observed fact,
deterministically projected into catalog, search, governance, and system-table
views. This ADR records the architectural decisions that bind every future
lineage slice, before any implementation broadens.

## Decision

Lineage is append-only observed fact projected into derived, read-only views.

1. Lineage observations are append-only events. Corrections append a new
   observation that supersedes a prior one (`supersedes_observation_id`,
   `correction_reason`); they never mutate or delete the original.
2. Observation envelopes carry explicit identity and replay metadata:
   `observation_id`, producer identity, producer event id/sequence,
   idempotency key, source cursor, ingest sequence, event time, and a raw
   payload reference with checksum. Replaying the same observations in
   source-cursor order against the same catalog identity snapshots must
   produce byte-equivalent projections.
3. OpenLineage is Arco's external lineage exchange envelope, not Arco's
   internal storage model. OpenLineage dataset `namespace`/`name` values are
   aliases and join hints resolved by a projector stage; Arco's internal graph
   keys are stable object IDs plus version/materialization identity.
4. Identity resolution is an explicit projector stage with recorded as-of
   context (resolver version, catalog snapshot token, candidate evidence) and
   enumerated statuses: `resolved`, `resolved_by_materialization`,
   `unresolved_external`, `ambiguous`, `stale_alias`, `deleted_object`,
   `replaced_object`, `unsupported_format`. Ambiguous or stale resolution is
   never silently projected as an exact edge.
5. Soundness is enumerated, never fabricated: `exact`, `table_only`,
   `dropped_column_facet`, `partial_input_stats_only`,
   `unsupported_plan_shape`, `unresolved_reference`, `identity_ambiguous`,
   `external_unresolved`. When a column-lineage facet is unsound, the whole
   facet is dropped; table-level lineage remains when table-level identity is
   sound. There is no name-only fallback that manufactures precise-looking
   column edges.
6. Lineage projections (`system.lineage.*`) are derived, watermarked,
   redacted, replayable read views. They are never mutation authority and
   never enforcement inputs (program rule 4; consistent with ADR-037 and
   ADR-039 rule 6).
7. Orchestrators and workers emit observations; they do not mutate catalog
   rows to publish lineage (program rule 9). The API/ingest boundary owns
   lineage write semantics.
8. Asserted governance facts (scanner/user tags) and inferred propagation
   (derived by traversing lineage edges) are separate record families;
   inferred records are always marked inferred with their derivation
   metadata.
9. Raw high-volume lineage observations do not enter the Tier-1 control-store
   tranche; they follow the event-family separation in the design doc
   (orchestration telemetry, lineage observations, and catalog/governance
   events keep separate hot paths, retention, and compaction cadence).
10. The legacy six-field `system.lineage.edges` table remains the
    compatibility surface during migration. Its rows fold with
    first-write-wins dedup by edge id; the L0 slice makes edge ids
    content-derived so duplicate client POSTs converge to one row. Schema
    evolution for richer projections is additive only, per ADR-006 and the
    [golden-schema plan](../plans/2026-07-30-lineage-l0-schema-plan.md).

## OpenLineage Compatibility

An OpenLineage RunEvent maps to an Arco lineage observation as follows: the
event's `run`/`job` identity becomes producer run/job correlation fields;
input/output datasets become resolution-pending alias records; column-lineage
facets become candidate column edges subject to the soundness contract; the
raw event is retained by reference (`raw_observation_path`,
`raw_observation_sha256`) behind redacted access. Compatibility adapters may
reshape native projections for Marquez/OpenLineage-adjacent consumers, but the
native Arco views own the semantics. Out of scope for L0: OpenLineage
ingestion endpoints, column-level extraction, resolver implementation, and any
Marquez-compatible read API.

## Consequences

- Every future lineage slice (L1–L6) implements against this contract:
  observation envelope first, resolution and soundness as data, projections
  replayable and watermark-carrying.
- The legacy edge surface stays writable and compatible while the observation
  model is built alongside it; no big-bang migration is required.
- Enforcement surfaces can never grow a dependency on lineage views without
  violating an accepted ADR, making the boundary testable.
- Deterministic edge identity makes duplicate ingestion idempotent at
  projection time, at the cost of collapsing intentionally repeated identical
  assertions (same source, target, type, and run) into one edge — repeated
  observations of the same logical dependency are the design intent.
- That collapse is lossy in two specific ways, and the loss is accepted rather
  than incidental. Because `run_id` participates in the edge id, every
  observation of `(source, target, edge_type, None)` — that is, every edge
  POSTed without a run id — folds into a single row, and first-write-wins
  dedup keeps the *first* `created_at`. So on the legacy
  `system.lineage.edges` surface: **multiplicity** is unrecoverable (the table
  cannot say whether a dependency was asserted once or ten thousand times),
  and **recency** is unrecoverable (`created_at` is the first observation, and
  no column records the latest, so the table cannot say whether a dependency
  is still being observed or was last seen months ago). A reader must not use
  this surface for freshness, staleness, or frequency questions.
- Callers that need observations kept distinct must supply a `run_id`, which
  re-separates them one row per run; callers that need full multiplicity and
  recency must wait for the L1+ observation model, where every observation is
  retained as an append-only event with its own event time and ingest
  sequence. The legacy table is a compatibility surface for *what depends on
  what*, not an observation log.
