> Status: Draft Phase 0 contract scaffold.
> Implementation status: Describes current baseline and proposed target semantics.
> Architecture status: The control-store path is prototype-approved only, not accepted production architecture.
> Compatibility status: Public/API exposure decisions are unresolved unless explicitly stated in this document.

# Projection Watermark Contract

Projection watermarks describe freshness of derived read models. They do not
grant mutation authority.

## Non-Authority Rule

System tables, lineage projections, search indexes, audit views, and derived
indexes are never mutation authority and never enforcement inputs.

Authorization and credential vending may use authoritative control state or a
separately validated compiled enforcement state in a future design. The derived
projection surfaces named in this document do not serve as enforcement inputs.

## Current Implemented Baseline

Current system-table, lineage, search, and orchestration views are derived from
their underlying authoritative event, ledger, or manifest-published state. They
may be rebuilt from authority and are not independent mutation roots.

## Proposed Target After Validated Cutover

For a future migrated control-store scope, projection workers consume authority
state, event archives, or projection outbox records and publish derived
artifacts. Each projection records the highest authority logical sequence it
contains.

Draft shape:

```text
ProjectionWatermark {
  scope,
  authority_sequence,
  projection_name,
  projection_version,
  published_at,
  source_checkpoint
}
```

The exact wire shape remains open.

## Surface Semantics

- `system.catalog.*`: query-optimized rows derived from authority.
- `system.lineage.*`: read-only lineage views derived from append-only
  observations.
- `system.access.*`: audit and governance views derived from authority and audit
  events.
- Search indexes: derived lookup structures, not authorization state.
- Derived indexes: rebuildable acceleration structures, not mutation roots.

Lineage wording must remain:

```text
append-only observations
  -> deterministic projections
  -> read-only views
```

Orchestrators must not mutate catalog rows to publish lineage.

## Freshness And Errors

Consumers that require projection freshness should compare the
`ProjectionWatermark` with the required authority sequence or token. If the
projection is too stale for a read contract, the surface should fail with
`ProjectionTooStale` or expose stale status according to the API contract.

This draft does not decide which public APIs expose projection watermarks in
bodies, headers, or system-table rows.

## Open Decisions

- watermark wire shape;
- projection versioning;
- default stale-read behavior by API surface;
- compatibility exposure for watermarks;
- projection retention and backfill rules;
- conformance tests proving deterministic rebuilds.
