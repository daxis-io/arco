# Catalog Inventory Snapshot Product Surface

## Status

Implemented as a narrow Arco-native API slice.

## Decision

Expose a product-facing catalog inventory descriptor instead of exposing raw
manifest JSON or creating a second catalog storage format.

The storage protocol remains:

1. `root.manifest.json` is the stable bootstrap entrypoint.
2. `catalog.pointer.json` selects the current immutable catalog manifest.
3. `manifests/catalog/{manifest_id}.json` selects the catalog snapshot.
4. `snapshots/catalog/{version}/` contains the Parquet inventory files.

The API product surface is:

```text
GET /api/v1/catalog/inventory
```

The response carries:

- `manifest_type = catalog_inventory_snapshot`
- `catalog_snapshot_version`
- `catalog_manifest_id`
- `published_at`
- safe counts for catalogs, schemas, tables, and columns
- object-family descriptors with stable query handles

It intentionally does not expose raw manifest paths, snapshot file paths, raw
ledger paths, grant internals, credential material, or storage policy payloads.

## Rationale

Catalog browsers and UI explorers need a stable, cheap answer to "what catalog
cut am I looking at?" They do not need direct access to the internal manifest
files. Keeping the inventory endpoint as a derived descriptor preserves the
pointer-first control-plane model while giving clients enough identity and
freshness data to coordinate paging, search, lineage, and query views.

## Follow-Ups

- Add object-level authorization filtering once the native catalog object authz
  surface is available for this endpoint.
- Add pagination endpoints or query-handle helpers if UI clients need first-page
  materialization rather than querying `system.catalog.*`.
- Keep UC/discovery compatibility routes as adapters over this Arco-native
  snapshot identity instead of making UC the authority.
