# Catalog read handoff (0.3.0)

Arco 0.3.0 exposes an operator-only catalog projection handoff at
`POST /internal/control-store/catalog-projection/urls`. The endpoint is absent
unless `ARCO_CONTROL_STORE_OPERATOR_ENDPOINTS` is enabled, is never mounted in
public posture, and requires the configured control-store operator group.

The request body is empty or `{}`. Tenant and workspace come only from the
verified request identity. Callers cannot select a path, historical version,
catalog root, or metastore. This profile serves the workspace catalog domain;
metastore-root handoff is not part of this contract.

## Descriptor version 1

The response is a single coherent read descriptor:

```json
{
  "descriptorVersion": 1,
  "scope": {
    "tenantId": "acme",
    "workspaceId": "analytics",
    "domain": "catalog"
  },
  "authority": {
    "kind": "controlV1",
    "manifestId": "immutable-authority-manifest-id",
    "sequence": 42
  },
  "projection": {
    "sequence": 42,
    "manifestPath": "control/v1/projections/catalog-parquet/00000000000000000042-immutable-authority-manifest-id/manifest.json",
    "manifestChecksumSha256": "64-lowercase-hex-characters",
    "manifestStorageVersion": "opaque-storage-version",
    "manifestEtag": "optional-storage-etag",
    "publishedAt": "2026-10-08T12:00:00Z"
  },
  "ttlSeconds": 900,
  "files": [
    {
      "path": "control/v1/projections/catalog-parquet/00000000000000000042-immutable-authority-manifest-id/catalogs.parquet",
      "format": "parquet",
      "checksumSha256": "64-lowercase-hex-characters",
      "byteSize": 1234,
      "rowCount": 1,
      "storageVersion": "opaque-storage-version",
      "etag": "optional-storage-etag",
      "url": "https://object-store.example/signed-url"
    }
  ]
}
```

`authority.sequence` is the authority cut observed by the materializer.
`projection.sequence` is the published catalog projection cut. Arco returns a
descriptor only when they agree with the committed catalog outbox sequence and
the outbox has no pending records. Legacy catalog authority uses its immutable
domain manifest identity and snapshot version for the same fields.

The `files` array contains exactly `catalogs.parquet`, `namespaces.parquet`,
`tables.parquet`, and `columns.parquet`. It never contains raw commits, audit,
authority, or caller-selected objects. Before returning, Arco checks every
file's presence and declared size, records its opaque storage version and ETag,
then checks those identities, the immutable manifest bytes, and the authority
cut again. Missing, replaced, lagging, failed, quarantined, or concurrently
advanced publication state returns `503 catalog_projection_unavailable` and no
descriptor.

The signed URLs are bearer capabilities valid for 900 seconds. Authorization
occurs when Arco issues the descriptor. Revoking the operator after issuance
does not revoke an already issued URL. A deployment that needs authorization
on every object request or range request must put that policy at its delivery
boundary.

Clients should verify each downloaded object's `byteSize` and
`checksumSha256`, and use ordinary HTTP range requests with any Parquet reader.
`storageVersion` and `etag` are evidence for the object observed at issuance;
they are opaque and must not be parsed.
