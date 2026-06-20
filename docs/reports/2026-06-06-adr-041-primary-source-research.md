# ADR-041 Primary-Source Research

Date: 2026-06-06

## Scope

This note records the primary-source research used for the first ADR-041
implementation slice: tiered object-storage orchestration event-log contracts,
L0 inbox bundle write semantics, and the authority boundary between ingestion,
compaction, manifests, optional DuckDB actors, and Quack.

## Sources and Implications

### Object-Store Conditional Writes and Consistency

- Google Cloud Storage request preconditions:
  https://docs.cloud.google.com/storage/docs/request-preconditions
  - `ifGenerationMatch` is the GCS compare primitive. The special generation
    value `0` implements create-if-absent for object writes.
  - ADR-041 L0 bundle, L1 segment, projection-file, shard-index, and manifest
    writes must continue to use Arco's `WritePrecondition::DoesNotExist` or
    `WritePrecondition::MatchesVersion` abstractions instead of ad hoc
    read-then-write checks.
- Google Cloud Storage consistency:
  https://docs.cloud.google.com/storage/docs/consistency
  - Object read-after-write, object metadata reads, and object listing are
    strongly globally consistent after a successful write response.
  - GCS listing can support anti-entropy discovery, but it should not become
    the foreground correctness path for scheduling, worker cleanup, or
    manifest visibility.
- Amazon S3 conditional writes:
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
  - `If-None-Match: *` is the S3 create-if-absent primitive; `If-Match` checks
    the current ETag before overwrite.
  - Arco's storage abstraction must preserve both semantics: create immutable
    artifacts by absent-key precondition and publish mutable heads by versioned
    compare-and-swap.
- Amazon S3 consistency:
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel
  - S3 provides strong read-after-write consistency for object PUT and DELETE,
    and strongly consistent HEAD/object metadata reads.
  - ADR-041 does not need a database to make object writes visible, but still
    needs conditional writes to prevent duplicate or stale writers.

### Object Notifications and Anti-Entropy

- Cloud Storage Pub/Sub notifications:
  https://docs.cloud.google.com/storage/docs/pubsub-notifications
  - Cloud Storage notifications are at-least-once, may be delayed, may be
    duplicated, are not ordered, and can be dropped after persistent delivery
    failure.
  - GCS notifications can wake compactors, but ADR-041 must keep scheduled
    sweeps or read-through repair for missed `_inbox/` bundles.
- Amazon S3 event notification ordering and duplicates:
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html
  - S3 event notifications are at-least-once and can arrive duplicated or out
    of order.
  - S3 notifications must be treated as hints. Bundle identity, event identity,
    and producer sequence numbers are still required for dedupe and replay.

### Parquet Schema and Versioning

- Apache Parquet logical type definitions:
  https://parquet.apache.org/docs/file-format/types/logicaltypes/
  - Parquet writers need stable physical types plus logical annotations, and
    readers must account for backward-compatibility rules around older
    encodings such as `ConvertedType`.
  - ADR-041 Parquet segments and projections should use explicit
    `schema_version`, stable required identity columns, nullable additive
    fields for evolution, and golden schema tests before Parquet-backed L1/L2
    publication is marked shipped.

### Leases and Fencing

- Kubernetes coordinated leader election:
  https://kubernetes.io/docs/concepts/cluster-administration/coordinated-leader-election/
  - Kubernetes Lease objects model holder identity, acquire/renew times,
    duration, transition counters, and optimistic concurrency through
    `resourceVersion` so only one contender updates an expired lease.
  - ADR-041 should keep Arco's existing lock/fencing model: a lease record with
    monotonic fencing token, expiry, and CAS/version preconditions. The token
    must be carried into L1 shard-index commits, L2 projection commits, and
    manifest publication.

### DuckDB and Quack Boundaries

- DuckDB Quack overview:
  https://duckdb.org/docs/current/quack/overview
  - Quack is HTTP-based, client-driven, and as of the current docs is still
    beta with protocol/defaults subject to change.
  - ADR-041 should not require Quack for durability, worker callbacks, or
    public ingestion. Any Quack path must be private and fallback-capable.
- DuckDB Quack security:
  https://duckdb.org/docs/current/quack/security
  - A Quack server exposes the full SQL surface of the underlying DuckDB
    session, including writes to visible tables. The docs recommend not
    exposing it directly beyond local-only deployments and provide separate
    authentication and authorization callbacks.
  - Quack must remain an internal accelerator behind custom authorization. It
    cannot be treated as a tenant-facing authority surface or as a bypass for
    callback token and active-attempt validation.

## Implementation Consequences for This Slice

- Keep object storage as the durable authority.
- Preserve current callback and one-object ledger behavior until L0-to-L1
  promotion has tests.
- Add explicit L0 inbox bundle schema and writer contracts using create-if-absent
  writes.
- Reject malformed bundles before storage write: empty IDs, mixed
  tenant/workspace/run scope, duplicate `event_id`, duplicate
  `producer_id + producer_seq`, and unsupported schema versions.
- Do not claim L1 segments, L2 projections, active DuckDB actors, trusted direct
  upload, or Quack fan-in are implemented by this slice.
