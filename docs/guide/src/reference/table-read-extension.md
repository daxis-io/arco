# Bounded Table-Read Extension

Arco serves two owner read profiles on the same endpoints. Use an explicit
`delta_version` for a [pinned managed Delta snapshot](#pinned-managed-delta-snapshot).
The request shape below selects the single-object Parquet profile.

## Single-object Parquet profile

`POST /api/2.1/unity-catalog/extensions/table-reads/{table_id}` is an
Arco-native, default-disabled extension for a caller that needs one small,
current, owner-authorized Parquet object. SQL execution stays with the reader.

`POST /api/2.1/unity-catalog/extensions/table-descriptors/{table_id}` is the
metadata companion for the same object. It performs the same current `SELECT`,
governed-storage, catalog, object, and watermark checks and returns a JSON
descriptor. Its optional body has the same
`expected_snapshot_id` pin as a table read. The response includes the pinned
`input_identity`, a semantic metadata digest bound to that snapshot, schema,
grain, units, meaning, loading time, source-data cutoff, enforcement, and
limitations. Completeness is `unknown`. Loading time and source-data cutoff
are separate owner-supplied facts.

Owner-supplied semantic metadata comes from catalog table and column
descriptions and the optional table properties below.

| Property | Value |
| --- | --- |
| `arco.dataset.grain` | Nonempty owner-supplied grain text. |
| `arco.dataset.units` | JSON object of owner-supplied units. |
| `arco.dataset.loaded_at` | Owner-supplied RFC 3339 loading timestamp. |
| `arco.dataset.source_data_cutoff` | Owner-supplied RFC 3339 source-data cutoff timestamp. |

Absent or malformed values appear as unavailable facts. Keep them unavailable
in Turntable.

Arco resolves the authenticated tenant, workspace, and principal from its
existing request middleware. It evaluates the current metastore-backed `TABLE
SELECT` grant and storage-governance read decision, reads one cataloged object,
and repeats those checks before returning bytes. Arco resolves the location,
credential, and authorization from its own configuration and catalog.

## Enablement and storage mapping

The extension is off by default. These settings enable the UC facade and the
extension.

```text
ARCO_UNITY_CATALOG_ENABLED=true
ARCO_UNITY_CATALOG_TABLE_READS_ENABLED=true
ARCO_STORAGE_BUCKET=s3://table-read-bucket
```

`ARCO_STORAGE_BUCKET` remains the only physical-storage configuration. The
extension derives the backend identity from the configured adapter.

- `s3://bucket` and `s3a://bucket` map to `s3://bucket` and work with the
  existing S3/Moto adapter configuration.
- `gs://bucket`, `gcs://bucket`, and a bare bucket name map to `gs://bucket`.
- S3 and GCS adapters use the bucket identity. Catalog locations must use
  that bucket as the root.
- Azure mapping requires a configured account authority for governed
  `abfss://` locations and is unavailable in this profile.

For an authenticated scope `tenant=acme`, `workspace=analytics`, use the
following location shape.

```text
s3://table-read-bucket/tenant=acme/workspace=analytics/<relative>.parquet
```

The object must be a single non-directory `.parquet` object under that exact
scope prefix. Unknown roots, other scopes, traversal-like paths, provider URLs,
and non-Parquet formats are rejected. Arco reads the derived relative path only
through its existing `ScopedStorage` adapter.

## Request and response

The body may be empty or `{}`. A non-empty body must be JSON and may contain
the optional pin below.

```json
{"expected_snapshot_id":"64-lowercase-hex-characters"}
```

If present, the pin must equal the current response snapshot or the route
returns `409 table_read_snapshot_mismatch`.

A successful response contains raw bytes with the following headers.

```text
Content-Type: application/vnd.apache.parquet
Cache-Control: no-store
```

These headers carry the Parquet identity and authorization evidence.

```text
X-Arco-Table-Read-Version: 1
X-Arco-Table-Read-Tenant
X-Arco-Table-Read-Workspace
X-Arco-Table-Read-Principal-SHA256
X-Arco-Table-Read-Table-Id
X-Arco-Table-Read-Table-Fingerprint
X-Arco-Table-Read-Object-Version-SHA256
X-Arco-Table-Read-Content-SHA256
X-Arco-Table-Read-Byte-Size
X-Arco-Table-Read-Physical-Schema-SHA256
X-Arco-Table-Read-Snapshot-Id
X-Arco-Table-Read-Permission-Watermark
X-Arco-Table-Read-Authorization-Audit-Id
X-Arco-Table-Read-Enforcement: table_select,storage_governance;row_filter=unsupported;column_mask=unsupported
```

The response exposes the opaque object version as a SHA-256 digest.

## Identity rules

All digests are lowercase SHA-256 hex. The snapshot ID is the digest of UTF-8
minified JSON emitted by declaration-ordered `serde_json`, with fields in this
exact order below.

```json
{"domain_version":1,"tenant":"…","workspace":"…","table_id":"…","table_fingerprint":"…","object_version_sha256":"…","content_sha256":"…","byte_size":123,"physical_schema_sha256":"…"}
```

The response carries `permission_watermark` and `authorization_audit_id`
separately from snapshot identity. An unrelated grant event can advance the
watermark while the same pin remains valid.

`table_fingerprint` is SHA-256 over declaration-ordered minified JSON containing
`domain_version`, the catalog table's ID, namespace ID, name, description,
location, format, type, properties, create/update timestamps, and all columns.
Columns are ordered by ordinal and then ID. Each includes ID, table ID, name,
data type, nullability, ordinal, and description.

`physical_schema_sha256` is SHA-256 over three binary components.

1. ASCII `arco.table-read.physical-schema.v1` followed by one NUL byte.
2. A big-endian `u32` root-first Parquet `SchemaElement` count.
3. For each element, a big-endian `u32` compact-Thrift encoded length followed
   by that deterministic Apache Thrift Compact encoding.

The route checks the initial object size before fetch, caps the returned object
at 8 MiB, validates Parquet metadata and its physical schema, then repeats the
catalog, authorization, object-version/size, and metastore-watermark fences
before disclosing the already-read bytes.

Before the Parquet decoder sees footer metadata, Arco requires leading and
trailing `PAR1` magic, a footer no larger than 1 MiB, Compact-Thrift containers
no larger than 65,536 items and no more than 65,536 container elements in
total, structural nesting no deeper than 64, varints no longer than 10 bytes,
and length-prefixed values that remain within the footer. This is a resource
preflight. The Parquet decoder then validates the metadata and physical schema. Callers should apply
equivalent input bounds before parsing returned bytes themselves.

## Authorization and enforcement boundary

The enabled profile enforces only current table `SELECT` and current governed
storage read authority through Arco's production `MetastorePermissionSource`
and temporary-table-credential decision path. A stale or unreadable projection,
revoked grant, missing location binding, or advanced ledger watermark denies
closed.

The route returns the full bounded object and reports row filters and column
masks as unsupported. Arco rejects unknown request fields, including
enforcement selectors.

## Stable errors

| Status | Meaning |
| --- | --- |
| 400 | Invalid body, pin, Parquet format, catalog format, or configured-root/scope mapping. |
| 403 | Current Arco principal lacks the required authorization. |
| 404 | The authorized catalog table or selected object is absent. |
| 409 | The catalog/object changed during the read, or the expected snapshot does not match. |
| 413 | Request body or selected object exceeds its bounded limit. |
| 503 | Storage mapping, storage-governance projection, current watermark, object version, or storage operation is unavailable. |

The public messages are stable reason codes. Detailed storage and authority
errors stay in Arco logs and audit correlation only.

## Pinned managed Delta snapshot

Use the same enablement flags and the configured S3-compatible bucket.
Send an Arco bearer token scoped to the table's tenant and workspace.
Resolve the registered table by its Arco table ID. Its native coordinator
uses `tables/<Arco table UUID>` in that scope. Catalog registrations can
omit the location or declare the exact configured bucket and canonical path.

The coordinator records the staged log digest in the reservation CAS. Arco
only serves log digests covered by the finalized version. Each `add` action
must include the exact data-file digest as a lowercase SHA-256 string at
`tags["arco.content.sha256"]` and the correct `size`. Arco verifies both
against the returned bytes, including on the first descriptor request.
Historical coordinator state without these log digests, data files without
committed digests, and custom roots lack the identity this profile requires.

### Approve an analytical root and allocate a table

Set `ARCO_MANAGED_DELTA_ROOT` to the approved S3 bucket and nonempty prefix.
Arco derives the workspace path from authenticated tenant and workspace
claims and stores approval in the metastore. The approval endpoints require
current `MANAGE` on the metastore.

- Send `PUT /api/2.1/unity-catalog/storage-governance/managed-roots/{root_id}`
  with a canonical UUIDv7 and an empty body. A new approval returns 201 and
  the managed root record. Identical retries return 200 with the same
  record. Each workspace can have one active managed root.
- Send `DELETE` to the same path to revoke the exact root incarnation. The
  response returns the record with lifecycle state `deleted`. The call also
  works after the analytical root configuration is removed. Use a fresh root
  ID for a new approval. Tables bound to the revoked root stay blocked.

Both operations publish the current storage-governance projection before
returning success. A retry also repairs publication after a committed event
whose publication failed.

The first allocation profile uses `POST /api/v1/managed-delta/ensure` to
create `turntable.verification.managed_delta_fixture` with non-null BIGINT
columns `id` and `amount`. Send the same `operation_id` (UUIDv7), `owner`
(authenticated principal), and `provisioning_fingerprint` on retry. The
owner needs `CREATE_TABLE` on the schema. Keep the returned table ID and
exact location. Native stage and commit require `MODIFY`, and the reads
below require `SELECT`. The catalog retains placement and coordinator
state while the approved analytical prefix holds the Delta log and data.

### Describe and read

1. POST `/api/2.1/unity-catalog/extensions/table-descriptors/{table_id}` with
   `{"delta_version":0}`. For a repeat descriptor, include the original
   `expected_snapshot_id`.
2. Verify the returned `input_identity` matches the operator's table and scope
   and the exact requested `delta_version`. The descriptor's `schema` comes
   from the pinned Delta log. Catalog descriptions and owner-supplied
   semantic properties provide the optional descriptive facts. Completeness
   is `unknown`.
3. POST `/api/2.1/unity-catalog/extensions/table-reads/{table_id}` with
   `{"delta_version":0,"expected_snapshot_id":"<descriptor input_identity.snapshot_id>"}`.
   Both endpoints take JSON and return `application/json` with
   `Cache-Control: no-store`. The byte read requires the pin.
4. Check that the response `input_identity` exactly equals the descriptor.
   Verify the manifest and each decoded object. Materialize the objects in a
   new private directory using the validated relative paths and open it with
   the native reader at the explicit version. Compare the version and Delta
   metadata ID reported by the engine to `delta_version` and `delta_table_id`.
   The native reader replays the included log and reads its active Parquet files.

The Delta `input_identity` contains `format: "delta"`, `bundle_version: 1`,
`tenant`, `workspace`, `table_id`, `table_fingerprint`, `delta_table_id`,
`delta_version`, `byte_size`, `manifest_sha256`, and `snapshot_id`.
`byte_size` is the sum of decoded object lengths. The `snapshot_id` is
SHA-256 over the input identity with that field removed. It uses Arco's
canonical JSON with sorted object keys, compact UTF-8 and integer numbers.
The catalog fingerprint includes the incarnation and current catalog
metadata, so a catalog change can invalidate a pin even for the same
Delta version. A later append leaves earlier pinned identities intact.

The read response has `input_identity`, `objects`, and `authorization`.
Each object has `path`, `byte_size`, `sha256`, and `data_base64` (standard
base64). To verify `manifest_sha256`, canonicalize and hash the ordered
`objects` array with `data_base64` removed from each entry. Order is log
version followed by lexicographic data-file path. The current permission
watermark, audit event ID, and principal digest in `authorization` are
outside snapshot identity. Keep the original version and pin on retries.

Arco checks current table `SELECT`, the applicable storage-governance
binding and active credential, and a final metastore watermark on every
descriptor and read. Every returned file must resolve to the governed
authority of the table root. Arco fetches through the bucket adapter it
configures with its service identity. The reader receives relative names,
bytes, and digest evidence. The trusted consumer runs SQL against its
private Delta materialization and applies its own execution limits.

### Supported Delta profile and limits

- Reader protocol 1 and writer protocol 2. Version 0 contains the only
  protocol and metadata actions. Optional `commitInfo` and unique,
  data-changing `add` actions are supported.
- An unpartitioned, fixed, flat Parquet schema with at most 128 fields of
  type `date`, `long`, `integer`, `boolean`, or `string`. Format options,
  Delta table configuration, and schema-field metadata must be empty.
  The only supported catalog properties are the owner semantic properties
  listed above.
- Log replay starts at JSON version 0 and ends at the explicit version, up
  to version 15. Each log is at most 256 KiB. Up to 32 data files and 8 MiB
  of decoded log and data bytes may be returned per request. The JSON
  bundle fits within 12 MiB. The request body is capped at 1 KiB and the
  Delta resolution and disclosure path has a 15-second deadline.
- Data paths are flat ASCII `.parquet` basenames. The stem starts with an
  ASCII letter or digit and is at most 120 bytes of letters, digits,
  dots, underscores, or hyphens. Case-insensitive duplicates are rejected.
- Checkpoint replay, removes, schema changes, per-file statistics, table
  features such as deletion vectors or column mapping, row filters, and
  column masks are outside this profile. The descriptor reports full-table
  `SELECT` and governed storage as the supported enforcement.

### Delta errors

| Status | Public error message or reason |
| --- | --- |
| 400 | Invalid request or version, missing read pin, unsupported log or feature, or an object path outside the supported table-root shape. |
| 401 | JWT authentication required. |
| 403 | Current `SELECT` or governed storage authority denied. |
| 404 | The version has yet to finalize or a required committed object is absent. |
| 409 | `table_read_snapshot_mismatch`, `delta_read_object_identity_mismatch`, missing committed identity evidence, or a change in catalog identity. |
| 413 | The request or snapshot exceeds a profile limit. |
| 503 | The storage mapping, authority, coordinator, or storage backend is unavailable, or the deadline was exceeded. |

The production-router proof uses a local RS256 test issuer, real catalog
and metastore records, and MinIO through the S3 adapter. The native reader
test materializes the owner's JSON bundle and calls `DeltaTable(path,
version=...)` through delta-rs. The proof exercises the two published orders
versions after the append and validates the resulting revenue and row counts.

## Workspace event streams

`POST` and `GET /api/v1/event-streams/{streamId}/events` append and read
structured events. Arco uses verified JWT claims to resolve an exact tenant
and Workspace and an operator-approved binding for the stream. Grant
`TABLE MODIFY` to the producer and `TABLE SELECT` to the reader on the
stream's table. Use a managed Delta table in governed S3 storage with an
unset location or `s3://<bucket>/tenant=<tenant>/workspace=<workspace>/tables/<table-id>/`.

Set `ARCO_EVENT_STREAM_BINDINGS` to a JSON array of bindings. For example,
an integration can record sync results with this binding.

```json
[
  {
    "tenant": "example",
    "workspace": "acme",
    "stream_id": "integration-syncs",
    "table_id": "018e2567-1234-7cd1-8001-123456789aba",
    "schema": {
      "profile": "custom",
      "version": 1,
      "fields": [
        {"name": "connector_id", "type": "identifier"},
        {"name": "outcome", "type": "enum", "values": ["succeeded", "failed"]},
        {"name": "records_written", "type": "integer"},
        {"name": "full_refresh", "type": "boolean", "nullable": true}
      ]
    }
  }
]
```

Register the four required columns below in order, followed by the approved
fields in configuration order. Identifier and enum fields use Delta `string`,
signed 64-bit integers use `long`, and booleans use `boolean`. Enums allow
only configured values. Identifiers start with an ASCII letter or digit and
contain up to 128 letters, digits, underscores, or hyphens. Nullable fields
can be omitted or supplied as `null`. Arco rejects extra, duplicate, and
invalid fields. Stream IDs and field names start with a lowercase ASCII
letter and contain up to 64 lowercase letters, digits, or underscores.
Stream IDs can also contain hyphens.

| Column | Delta type |
| --- | --- |
| `workspace_id` | `string` |
| `schema_version` | `integer` |
| `source_event_id` | `string` |
| `occurred_at_ms` | `long` |

Send the approved fields in `fields` alongside `schemaVersion`,
`sourceEventId`, and `occurredAtMs`. Use the binding's schema version, a stable
UUIDv7, and UTC epoch milliseconds. Arco derives the Workspace from the
authenticated scope.

The `200` receipt contains `sourceEventId` and `commitVersion`. Arco
returns it after the Delta version finalizes. Repeat the same event after an
uncertain response or a retryable `503`. Replaying its canonical body returns
the original receipt. A different body under the same ID in the same stream
returns `409`. Each stream gets its own idempotency scope. The first append
pins the table and schema, and a conflicting binding returns `409`. Arco
protects the pinned table from raw Delta commit routes across restarts and
binding changes. Apply the same bindings on every replica. A new schema or
table incarnation uses a new stream ID.

`GET` returns `events` in descending commit order, `publishedVersion`, and
`nextBeforeVersion`. Each row contains `workspaceId`, `commitVersion`, and the
accepted `event`. Arco verifies the committed log and data file before returning
a row. The optional `limit` defaults to 25 and ranges from 1 to 50. To continue
a page, pass its `publishedVersion` and use `nextBeforeVersion` as
`beforeVersion`. Version 0 represents an empty stream. The watermark measures
finalized Arco commits. The current write profile commits one event at a time.

### Pin a stream for a Flow run

Send `POST /api/v1/event-streams/{streamId}/pins` with `{}` to pin the
current published version, or send `{"publishedVersion":16}` to select
an exact version. Arco returns `pinId` and `publishedVersion`. Repeating
the request for the same version of the same stream returns the same pin.

Read `GET /api/v1/event-streams/{streamId}/events?pinId=<pin-id>` and
continue with the same pin and `beforeVersion` from `nextBeforeVersion`.
Every page checks the reader's current `TABLE SELECT` and storage
governance. The pin binds the version to the Workspace stream, table,
schema and storage root. This uses the event reader's page limit and works
beyond the bounded Delta descriptor's version limit.

### Turntable query activity

Use `{"profile":"logfood_v1"}` with stream ID `query-activity` and a managed
table named `activity_events`. The existing `ARCO_ACTIVITY_TABLE_BINDINGS`
mapping from `tenant/workspace` to table UUID also defines this stream. With
the UC facade enabled, `/api/2.1/unity-catalog/extensions/activity-events`
serves the same rows and receipts with the activity adapter's flat snake-case
request and response format.

After the four required columns, add `stage`, `outcome`, `surface_id`,
`release_id`, and `operation_id` in that order as strings. `release_id` is
nullable. The schema version is 1. The supported stage and outcome pairs are
`admission` with `admitted` or `denied`, `execution` with `succeeded` or
`failed`, and `delivery` with `authorized` or `denied`. Delivery records the
server decision. Turntable owns its investigation grant and capture coverage.

## Hourly Logfood Usage

Register a governed, managed Delta table named `surface_usage_hourly` in
the same Workspace as `query-activity`. Its column order matches the Flow
artifact. Use `string` for `workspace_id`, `surface_id`, and `release_id`,
and `long` for the remaining columns. Only `release_id` is nullable.

```text
workspace_id, hour_start_ms, surface_id, release_id, source_delta_version,
admitted, admission_denied, executed, execution_failed, delivery_authorized, delivery_denied
```

Set `ARCO_USAGE_TABLE_BINDINGS` to a JSON array with `tenant`, `workspace`,
`table_id`, and `transform_sha256` for each destination. Use the SHA-256 of
the approved transform and a distinct `query-activity` source table. Apply
the binding on every writer. Grant the publisher `TABLE MODIFY` and the
Usage reader `TABLE SELECT` on the destination table.

For a manual materialization, pin `query-activity` and set the execution
payload for `analytics.surface_usage_hourly` before triggering the Flow run.
The accepted plan preserves these values for every attempt.

```json
{"streamPinId":"<pin-id>","hourStartMs":1800000000000,"transformSha256":"<approved-sha256>"}
```

The worker reads all pages at that pin under its own Activity reader
credential. Existing plans can still use `inputIdentity` and `inputSha256`.
After the worker completes, send `POST /api/v1/usage/hourly` with
`{"run_id":"…","attempt_id":"…"}`. Arco checks the pinned Activity scope
and version, the approved transform, and the visible, successful attempt
with its verified Parquet artifact. A single Delta commit selects the new file
and removes the previous file for that UTC hour. Other hours retain their selected files.
The receipt includes the source and destination table versions and the Flow
attempt. Repeat a publication to retrieve its original receipt. A stale
attempt, an older unpublished source version, or a changed output for a
published source version returns `409`.

`GET /api/v1/usage/hourly` reads the latest published hour by default. Pass
`hour_start_ms` and `published_version` to read a specific hour from a fixed
Delta snapshot. Continue with its `next_offset` and the same hour and version.
The default page size is 25 and the maximum is 50. Use `GET
/api/v1/usage/hourly/receipt?hour_start_ms=…&source_delta_version=…` to look up
an original finalized receipt. The Usage watermark tracks committed
materializations. The source watermark tracks the Activity rows included in
the calculation. The initial profile supports up to 4,096 groups per hour and
an 8 MiB Parquet artifact.
