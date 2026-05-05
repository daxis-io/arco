# Object-Native Catalog Architecture

This page is the production target for Arco Catalog as an object-storage-resident
metadata engine. It is a north-star architecture, not a claim that every subsystem
is implemented in the current repository. For repo-local implementation status, use
the [control-plane scope scorecard](./control-plane-scope.md).

## Design Thesis

Arco Catalog should be a per-entity, CAS-headed, immutable-snapshot catalog stored
entirely in object storage.

The catalog does not use one global metastore snapshot as the common commit point.
Instead, it uses:

- deterministic name bindings for direct table resolution
- small mutable head objects as compare-and-swap registers
- immutable snapshots, schemas, manifests, commit records, and policy snapshots
- object-native LSM indexes for listing and secondary lookup
- Delta Lake data and `_delta_log/` files as the table data plane

The result is a Unity Catalog-like namespace and governance surface without an
always-on transactional metastore database. Delta table data stays portable, while
Arco adds a catalog-side metadata graph, manifest planning layer, and governance
surface around it.

## Non-Negotiable Principles

All durable catalog state lives in object storage. Arco must not require Postgres,
MySQL, DynamoDB, Spanner, FoundationDB, Hive metastore, or any other always-on
transactional metadata database for durable catalog correctness.

Only tiny pointer objects are mutable. Examples:

```text
tables/<table_id>/head.json
schemas/<schema_id>/head.json
catalogs/<catalog_id>/head.json
kv/<index>/<shard>/head.json
```

Everything referenced by those heads is immutable and written with
create-if-absent semantics.

Hot lookup paths must not use object-store listing. Resolving:

```sql
SELECT * FROM sales.curated.orders;
```

must map to known object keys for the name binding and table head. It must not list
`_delta_log/`, `catalog/`, `schemas/`, or `tables/`.

Table commits serialize per table. A write to `orders` does not update a global
catalog root, and it does not contend with a write to `customers`.

Arco has two Delta compatibility modes:

| Mode | Use Case | Authority |
|---|---|---|
| Authoritative catalog-managed Delta | Production Arco-managed tables | Arco table head is the commit point |
| Mirror / accelerator | Existing Delta tables with outside writers | Delta log remains authority; Arco is a cached mirror |

Authoritative mode is the production target. Mirror mode is useful for bootstrap,
migration, and mixed-writer environments, but it cannot provide full catalog
governance if writers can bypass Arco.

## Production Topology

```text
Query engines
  Spark / DataFusion / DuckDB / Flink / Trino adapter
      |
      | Arco Catalog API
      v
Arco catalog client
  - name resolver
  - policy evaluator
  - snapshot resolver
  - manifest planner
  - object-store CAS committer
  - metadata cache
      |
      | GET / HEAD / PUT-if-absent / PUT-if-match
      v
Metadata object store
  names/
  tables/<table_id>/head.json
  tables/<table_id>/snapshots/*.json
  tables/<table_id>/schemas/*.json
  tables/<table_id>/commits/*.json
  tables/<table_id>/manifests/**/*.parquet
  kv/<index>/<shard>/head.json
  kv/<index>/<shard>/levels/**/*.sst
  security/
  audit/
      |
      | resolved Delta table location
      v
Data object store
  <table>/_delta_log/*.json
  <table>/_delta_log/*.checkpoint.parquet
  <table>/_delta_log/_staged_commits/*
  <table>/part-*.parquet
  <table>/deletion-vector files
```

The optional gateway or serverless function is not durable metadata. It can
authenticate callers, vend scoped credentials, and emit audit records, but object
storage remains the source of truth.

## Storage Layout

A production metadata root should be tenant-scoped and versioned:

```text
metastores/prod/v1/
  _format.json
  tenants/
    t_acme/
      names/
        catalogs/
        schemas/
        tables/
      entities/
        catalogs/<catalog_id>/head.json
        schemas/<schema_id>/head.json
      tables/
        <table_id>/
          head.json
          snapshots/
          schemas/
          commits/
          manifests/
            roots/
            lists/
            files/
            deletes/
          indexes/
            partitions/
            files/
          transactions/
      kv/
        namespace/
        grants/
      security/
      audit/
```

Name bindings are deterministic direct objects. A table name maps to a binding key
derived from tenant, normalized catalog, normalized schema, and normalized table.
The binding points to the stable table ID and table head path.

Table heads are the per-table CAS register. They stay small and reference larger
immutable content:

```json
{
  "format": "arco.table-head.v1",
  "tenant_id": "t_acme",
  "table_id": "tbl_01J8Z8Y6C6A5BNM",
  "current_name": "sales.curated.orders",
  "state": "active",
  "catalog_sequence": 1042,
  "delta_version": 918,
  "table_location": "s3://lake/sales/curated/orders/",
  "commit_mode": "catalog_managed_delta",
  "snapshot_ref": "tables/tbl_01J8Z8Y6C6A5BNM/snapshots/00000000000000001042-a6c9.snapshot.json",
  "schema_ref": "tables/tbl_01J8Z8Y6C6A5BNM/schemas/00000000000000000017-b91f.schema.json",
  "manifest_root_ref": "tables/tbl_01J8Z8Y6C6A5BNM/manifests/roots/00000000000000001042-cc02.root.json",
  "parent_head_hash": "sha256:7bb1..."
}
```

The target size for a table head is under 256 KiB. Large fields must be referenced,
not embedded.

## Snapshot And Manifest Model

Delta already stores AddFile and RemoveFile actions, checkpoints, protocol metadata,
and file-level statistics. Arco materializes the current table state into sidecar
manifests so the catalog read path does not need to replay the Delta log before
planning ordinary reads.

The manifest hierarchy is:

```text
snapshot.json
  -> manifest root
      -> manifest-list shards
          -> file manifests
```

Manifest roots are small JSON or Parquet objects with coarse table and layer
summaries. Manifest-list shards contain one row per file manifest and carry partition
and column summary bounds for pruning. File manifests are Parquet files containing
active or recently removed Delta file rows, including enough preserved Delta action
metadata to hydrate an exact scan state.

Snapshots are layered:

```text
base manifests
+ delta add manifests
- delete manifests
= active file set for snapshot N
```

Append commits write only new L0 add manifests and a new manifest root. Delete,
update, and merge commits add both add and delete overlays. Compaction later folds L0
and delete overlays into larger L1/L2 manifests without changing data files.

Manifest compaction is mandatory for production performance. Correctness can survive
lagging compaction because readers can apply overlays, but performance eventually
regresses to log-like replay if L0 and delete layers are never compacted.

## KV And Listing Indexes

Direct deterministic objects handle hot exact lookup:

```text
table name -> table_id + table_head_path
table_id   -> table head
```

Object-native LSM indexes handle listing and secondary access patterns:

- schema table listing
- catalog listing
- tag and owner lookup
- grant lookup
- partition point lookup
- file path lookup
- transaction idempotency

Each index shard has one small mutable head and immutable SST segments:

```text
kv/namespace/shard=0007/
  head.json
  levels/
    L0/*.sst
    L1/*.sst
    L2/*.sst
```

Lookups read the shard head, inspect segment key ranges and filters, then range-read
the relevant SST block. They do not list the bucket.

## Read Path

For:

```sql
SELECT *
FROM sales.curated.orders
WHERE event_date = DATE '2026-04-24'
  AND tenant_id = 'acme';
```

the strict read path is:

1. Normalize `sales.curated.orders`.
2. GET the deterministic table-name binding.
3. GET the table head.
4. Authorize from the pinned policy snapshot.
5. Load the immutable table snapshot and schema, unless cached or inlined.
6. Use partition and manifest summaries to find candidate manifest shards.
7. Load candidate file manifests in parallel.
8. Return a scan plan containing Delta AddFile rows, deletion vectors, schema,
   policy filters, and storage options.

Name and snapshot resolution are O(1) object reads. File planning is proportional to
candidate manifest shards, not historical Delta log length. Full-table scans still
need to consider all relevant file manifests; no metadata system makes that O(1).

## Write Path

Authoritative catalog-managed Delta uses the table head CAS as the commit point:

1. Read the current table head with its object version, generation, or ETag.
2. Validate authorization, schema, constraints, protocol features, idempotency, and
   optimistic concurrency conflict rules.
3. Write data files to the table location.
4. Build Delta commit actions.
5. Stage the Delta commit under `_delta_log/_staged_commits/` or store it inline in
   catalog metadata.
6. Write immutable Arco commit records, manifests, manifest root, and snapshot.
7. CAS `tables/<table_id>/head.json` to the new snapshot.
8. Publish the ratified Delta commit to `_delta_log/<version>.json`.
9. Update secondary indexes and audit projections asynchronously.

If the CAS fails, the writer reloads the latest head, validates conflicts against
the commits since its original base, rewrites staged metadata for the new Delta
version if needed, and retries.

Post-CAS Delta publish is repairable. Once the Arco head advances, Arco readers can
see the ratified commit through the catalog even if `_delta_log/<version>.json` has
not yet been published. A repair publisher can publish it later.

## Consistency And Failure Semantics

Per-table commits are serializable because exactly one writer can win the head CAS
from sequence N to N+1.

Readers get snapshot isolation by pinning immutable objects:

```text
table head -> table snapshot -> schema -> manifest root -> manifest shards
```

Once the snapshot is selected, concurrent commits do not affect that reader.

Expected failure behavior:

| Failure | Visible Result | Recovery |
|---|---|---|
| Data files written, commit fails | Orphan files | Orphan cleanup |
| Staged Delta commit written, CAS fails | Staged commit ignored | GC staged commits |
| Arco manifests written, CAS fails | Orphan immutable metadata | Reachability GC |
| Arco CAS succeeds, Delta publish fails | New Arco snapshot visible | Publish repair |
| Secondary index update fails | Exact lookup remains correct | Rebuild from heads/snapshots |
| Direct writer bypasses Arco | Catalog can diverge | Block by IAM; detect and quarantine |

Object-store conditional writes are a production prerequisite. Without
create-if-absent and compare-if-current object operations, this design is not safe.

## Governance Model

Arco mirrors the Unity Catalog object model where it is useful:

- metastore
- catalog
- schema
- table
- view
- volume
- external location
- storage credential
- function
- share, later

Governance state follows the same rule as table metadata: immutable policy/grant
snapshots plus small mutable heads. Credential records contain references to cloud
roles or secret handles, not long-lived cloud secrets.

There are three enforcement levels:

| Level | Enforcement | Use Case |
|---|---|---|
| Engine-enforced | Trusted engines evaluate policy snapshots | Internal controlled engines |
| IAM-enforced | Raw users cannot read/write table prefixes directly | Production data isolation |
| Credential vending | Stateless gateway issues scoped temporary credentials | UC-like external access |

The gateway can disappear without losing catalog state. It is an enforcement and
credential boundary, not the metadata database.

## Caching Strategy

Immutable objects can be cached by path plus content hash:

- snapshots
- schemas
- manifest roots
- manifest lists
- file manifests
- SST segments
- policy snapshots

Mutable heads and name bindings are cached only with ETag, generation, or object
version validation. Strict mode validates on every read. Bounded-staleness mode uses
a TTL. Session mode pins one snapshot for a query or session.

Resolver packs can materialize the table names and head paths for an entire schema
into one Parquet object for BI workloads that repeatedly query the same namespace.

## Multi-Table Transactions

The common path should stay per-table. Multi-table atomicity can be added with a
transaction marker only for workloads that require it.

One object, `transactions/<txn_id>/commit.json`, becomes the atomic visibility marker.
Participating table heads can hold both stable and pending snapshot references. A
reader that sees a pending transaction checks the marker:

```text
marker exists     -> use pending snapshot
marker not found  -> use stable snapshot
```

Cleanup later promotes pending snapshots to stable snapshots. This adds an extra
lookup only for tables with pending transactions and avoids a global metastore root
for ordinary reads.

## Implementation Phases

1. Read accelerator for existing Delta tables:
   register tables, bootstrap from Delta checkpoints, write Arco table heads,
   snapshots, base manifests, and DataFusion scan integration.
2. Authoritative catalog-managed commits:
   add staged Delta commits, table-head CAS, Delta publish repair, and conflict
   validation.
3. Object-native LSM indexes:
   add namespace listing, partition lookup, file lookup, transaction idempotency,
   and compaction.
4. Governance and enterprise features:
   add grants, policy snapshots, external locations, storage credentials,
   credential vending, audit logs, and system tables.
5. Advanced features:
   add multi-table transaction markers, catalog-level time travel, refs, lineage,
   search, and sharing.

## Relationship To Existing ADRs

This architecture composes existing Arco decisions:

- [ADR-030](../../adr/adr-030-delta-uc-metastore.md) defines the Delta and UC-like
  metastore framing.
- [ADR-031](../../adr/adr-031-unity-catalog-api-facade.md) defines the Unity
  Catalog API facade boundary.
- [ADR-032](../../adr/adr-032-immutable-manifest-pointers.md) defines immutable
  manifests plus pointer CAS.
- [ADR-034](../../adr/adr-034-fenced-head-published-control-plane-transactions.md)
  defines fenced head-published control-plane transactions.

The main difference is scope: this page describes the production target where those
patterns are extended down to independently serializable table entities and outward
to enterprise governance, secondary indexes, and Delta table planning.

## External Protocol References

- Delta Lake protocol, catalog-managed commit protocol:
  <https://github.com/delta-io/delta/blob/master/PROTOCOL.md#commit-protocol>
- Delta Lake catalog-managed tables overview:
  <https://delta.io/blog/2026-02-02-delta-catalog-managed-tables/>
- Amazon S3 conditional writes:
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html>
- Amazon S3 strong read-after-write consistency announcement:
  <https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/>
- Google Cloud Storage request preconditions:
  <https://docs.cloud.google.com/storage/docs/request-preconditions>
- Google Cloud Storage consistency:
  <https://docs.cloud.google.com/storage/docs/consistency>
- Apache Iceberg manifest and manifest-list specification:
  <https://iceberg.apache.org/spec/#manifests>
