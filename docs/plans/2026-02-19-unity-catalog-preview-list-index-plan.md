# Unity Catalog Preview List Index/Manifest Plan

## Goal

Define a production-ready design for index/manifest-backed listing for Unity Catalog preview CRUD (`catalogs`, `schemas`, `tables`) so list APIs no longer depend on expensive full-prefix object listing + per-object reads.

## Current State

- Source-of-truth records are stored as one JSON object per UC entity under tenant/workspace scoped storage.
- List endpoints currently:
  1. list all object keys for a prefix,
  2. sort in memory,
  3. read/deserialize page records.
- This is acceptable for low preview volume but does not scale well for high cardinality prefixes.

## Design Summary

Introduce per-scope **index manifests** that store list-ready entries for each collection:

- Catalogs index for a tenant/workspace scope.
- Schemas index per catalog.
- Tables index per catalog+schema.

List handlers read the relevant index manifest first and paginate directly from it. Object-prefix listing remains as fallback/repair path.

## Storage Layout

All paths are relative to tenant/workspace-scoped storage.

- Catalog index:
  - `unity-catalog-preview/index/catalogs.v1.json`
- Schema index:
  - `unity-catalog-preview/index/schemas/{catalog_name}.v1.json`
- Table index:
  - `unity-catalog-preview/index/tables/{catalog_name}/{schema_name}.v1.json`

Data records remain unchanged:

- `unity-catalog-preview/catalogs/{name}.json`
- `unity-catalog-preview/schemas/{catalog_name}/{name}.json`
- `unity-catalog-preview/tables/{catalog_name}/{schema_name}/{name}.json`

## Manifest Shape

```json
{
  "version": 1,
  "updated_at_epoch_ms": 1739999999999,
  "entries": [
    {
      "name": "main",
      "comment": "primary"
    }
  ]
}
```

Notes:

- `entries` stores full response-ready objects (`CatalogInfo`, `SchemaInfo`, `TableInfo`) to avoid N+1 entity fetch.
- Entries are sorted by stable key (`name` or `full_name`) before write.
- `version` is manifest schema version (not storage CAS version).

## Write Path (Create APIs)

For `POST /catalogs`, `POST /schemas`, `POST /tables`:

1. Write entity record file with `WritePrecondition::DoesNotExist` (source of truth).
2. Update index manifest with optimistic CAS:
   - `head + get` current manifest (if exists),
   - merge new entry idempotently (`insert if absent`),
   - `put` with `MatchesVersion(current_version)`,
   - retry bounded times on precondition conflicts.
3. If index update still fails after retry budget:
   - return success for create (entity exists),
   - emit warning + counter,
   - mark for lazy repair.

Rationale:

- Avoid failing successful creates because secondary index update races.
- Keep correctness anchored in entity files.

## Read Path (List APIs)

For each list endpoint:

1. Validate query first (`catalog_name`, `schema_name`, `page_token`, `max_results`).
2. Attempt index manifest read.
3. If index read succeeds:
   - paginate entries directly,
   - return response.
4. If index missing/corrupt/read error:
   - fallback to legacy object-prefix path,
   - rebuild index asynchronously or write-through on request path (bounded frequency).

## Consistency Model

- Source of truth: entity files.
- Index is a derived projection with eventual consistency under concurrent writers.
- Expected behavior:
  - read-after-write is guaranteed for single create response body.
  - list read-after-write is strongly consistent when index update succeeds, eventually consistent on fallback repair path.

## Concurrency and Race Handling

- Duplicate creates remain protected by `DoesNotExist` on entity file.
- Index updates use CAS loop to avoid lost updates.
- Merge is idempotent by natural key (`name` or `full_name`), so retries are safe.
- Repair operation must also use CAS and idempotent merge.

## Failure Modes and Mitigations

- Index write conflict storms:
  - bounded retries, exponential backoff/jitter.
- Index corruption (invalid JSON):
  - log error, fallback to legacy listing, repair by full rebuild.
- Partial outages:
  - keep list endpoint available through fallback path.

## Observability

Add metrics and structured logs:

- `uc_preview_index_read_total{result=hit|miss|error}`
- `uc_preview_index_update_total{result=ok|conflict|error}`
- `uc_preview_index_rebuild_total{result=ok|error}`
- `uc_preview_list_fallback_total{collection=*}`

Log fields:

- `tenant`, `workspace`, `request_id`, `collection`, `index_path`, `fallback_used`.

## Testing Plan

Unit tests:

- CAS merge idempotency.
- manifest sort stability.
- conflict retry and exhaustion behavior.
- parse/corrupt index fallback behavior.

Integration tests:

- list uses index when present.
- list falls back when index missing/corrupt.
- concurrent creates do not drop entries from index.
- repair path reconstructs expected index from entity files.

Regression tests:

- existing CRUD behavior remains unchanged.
- existing openapi compliance remains green.

## Rollout Plan

Phase 1:

- Add index manifest structs + helpers behind feature flag `unity_catalog.preview_index_enabled`.

Phase 2:

- Write-through index updates on create endpoints.
- Read-path index lookup with fallback to current listing behavior.

Phase 3:

- Add lazy/explicit rebuild command for existing scopes.
- Enable flag by default after soak and telemetry review.

## Acceptance Criteria

- List endpoints can serve from index manifests without full prefix listing in steady state.
- No loss of correctness for create/list conflicts and scope isolation.
- Fallback path guarantees availability if index is unavailable.
- Metrics/logging provide operational visibility into hit-rate and repair behavior.
