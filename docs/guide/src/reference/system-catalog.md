# Published Operational Projections

Arco publishes catalog and orchestration read models as immutable Parquet
artifacts selected by manifest pointers. These artifacts are derived from
authoritative state. They are not commit points for control-plane decisions.

Arco does not host a `system.*` SQL catalog. The former `/api/v1/query` and
`/api/v1/query-data` endpoints have been removed. A client that needs SQL must
bring its own engine and map published artifacts to its own table names.

## Reading the Projections

- Catalog clients use scoped REST reads or request signed URLs for allowed
  manifest-selected files through `/api/v1/browser/urls`.
- Orchestration clients use the run/task REST APIs. Authorized projection
  readers can use the published orchestration manifest and its artifacts. The
  manifest, not a bucket listing, selects the visible files.
- Hosts must authorize the tenant and workspace before handing a query engine
  access to artifacts. Readers must respect the selected manifest generation.
  Arco's signed URLs are limited to the minting allowlist.

The old `system.catalog.*`, `system.lineage.*`, and `system.orchestration.*`
names were aliases inside Arco's removed DataFusion runtime. They are not
current Arco API names. See [Catalog Run Index](./catalog-run-index.md) for the
org-scoped orchestration index and [API Reference](./api.md) for the endpoint
migration.

## Catalog Audit Projection

A catalog root bound to the `control/v1` authority publishes one audit row for
every acknowledged catalog mutation. The rows are immutable Parquet files, not a
SQL table. The retention design calls this projection `system.catalog.audit`;
like the aliases above, that name is not an Arco API name.

The projection exists only under the one control-bound catalog root selected
by `ARCO_CATALOG_CONTROL_V1_*` (see [Control-Plane Scope](./control-plane-scope.md)).
Legacy catalog roots have no audit projection.

### Layout

Paths are relative to the catalog root (`tenant=<tenant>/workspace=<workspace>/`):

```text
control/v1/projections/catalog-audit/dt=YYYY-MM-DD/{source_logical_sequence:020}-{intent_id}.parquet
```

- `dt=YYYY-MM-DD` is the UTC day of the row's `occurred_at_ms`. Day names are
  fixed-width, so they sort chronologically.
- `{source_logical_sequence:020}` is the mutation's committed logical sequence,
  zero-padded to 20 digits.
- `{intent_id}` is the projection intent id, which equals `operation_id`.

Each file holds exactly one row. No manifest lists these files, so a reader
lists a day prefix (for example
`control/v1/projections/catalog-audit/dt=2026-09-27/`) and reads every file in
it. Because no manifest selects them, `/api/v1/browser/urls` does not mint URLs
for them; the host grants its engine read access to the prefix after it
authorizes the tenant and workspace.

### Schema

`arco_catalog::parquet_util::catalog_audit_schema()` returns the exact
schema. Rust readers can decode a file with
`arco_catalog::parquet_util::read_audit_records`, which returns
`arco_catalog::parquet_util::CatalogAuditRow` values.

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| `record_version` | `UInt32` | no | Version of the source audit record; always `1` today. |
| `operation_id` | `Utf8` | no | Operation id; also the projection intent id. |
| `operation_family` | `Utf8` | no | Operation family. |
| `request_digest` | `Utf8` | no | SHA-256 hex digest of the canonical request. |
| `actor` | `Utf8` | no | Actor that issued the mutation; `api` when the request named none. |
| `occurred_at_ms` | `Int64` | no | When the adapter accepted the mutation, in milliseconds since the Unix epoch (UTC). |
| `logical_sequence` | `Int64` | no | Committed logical sequence of the mutation. |
| `authority_manifest_id` | `Utf8` | yes | Authority manifest that committed the mutation. Set on every row written today. |
| `logical_commit_id` | `Utf8` | yes | Reserved for the test-only bounded authority format. Always null today. |

`operation_family` is one of `create_catalog`, `patch_catalog`,
`delete_catalog`, `create_schema`, `patch_schema`, `delete_schema`,
`register_table`, `update_table`, `rename_table` or `drop_table`.

### Guarantees

- **Written before acknowledgement.** The projection materializer writes the
  file after the intent's catalog snapshot files and before the snapshot
  `manifest.json`, and acknowledges the intent only after the file exists.
  Every intent acknowledged since retention step 3 has its file, so trimming
  the outbox never loses an audit row. Intents acknowledged earlier on an
  existing format-9 root kept their audit record as a row in the authority
  KV (key tag 4); the materializer never revisits them, so they have no file.
- **Quarantined intents.** An intent the materializer quarantines may or may
  not have a file (for example, one quarantined for a divergent snapshot
  manifest after its audit file landed). A file that exists still describes a
  committed mutation. An intent whose payload is not the audit record of that
  intent is quarantined as `INCOMPATIBLE_PROJECTION_INTENT` before anything is
  written.
- **Immutable files.** A file is written only if the path is free. A
  redelivered intent that produces identical bytes is accepted; different
  bytes at the path fail closed and quarantine the intent.
- **Identity.** A row is identified by `(operation_id, logical_sequence)`, not
  by `operation_id` alone. A keyed request's operation id is deterministic per
  operation family and idempotency key. Its idempotency receipt answers
  replays with the original response for at least 24 hours. Once the receipt
  is purged, the same keyed request re-executes under the same `operation_id`.
  While the earlier intent is still in the outbox, the retry is refused with
  a 409 conflict and writes nothing; only after the earlier intent is drained
  and trimmed does the retry commit at a later logical sequence and produce a
  second row. See the known limitations in
  `docs/runbooks/control-store-worker.md`. Unkeyed requests get a fresh
  operation id each time.

### Retention

The scheduled control-store worker deletes day partitions strictly older than
the retention: 400 days by default, set by
`ARCO_CONTROL_STORE_AUDIT_RETENTION_DAYS` (at least 30). With the default, the
partition of day `D` is kept through day `D + 400` and deleted by the first
sweep on day `D + 401` (UTC) or later. Each run deletes at most
`ARCO_CONTROL_STORE_AUDIT_SWEEP_MAX_OBJECTS` files (default 4096) in path
order and continues on the next run, so a reader can see an expired partition
partly deleted. Names under the prefix that are not a well-formed day
partition are never deleted. Operating details are in
`docs/runbooks/control-store-worker.md`.

### Restores

A workspace restore writes no audit row yet. The audit record for the restore
mutation itself arrives with retention step 4.
