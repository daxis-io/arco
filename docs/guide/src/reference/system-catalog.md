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
