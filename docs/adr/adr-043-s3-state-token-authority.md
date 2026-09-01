# ADR-043: S3 StateToken Authority

## Status

Accepted

## Context

Arco's current catalog write path makes synchronous Parquet publication the
visibility boundary. The API appends a ledger event and waits for a separately
deployed compactor to publish a complete snapshot. That design also made the
compactor's dedicated identity the sole snapshot writer and led to a public
gRPC surface for typed calls into the service.

The `ArcoStateStore` work proved a smaller authority boundary: immutable
objects plus a conditional object-store head update can publish logical state
without making a projection part of the transaction. Arco's first GA target is
AWS, S3, API Gateway, and Lambda, where a resident writer or synchronous
compactor would defeat scale-to-zero operation. The authority format and state
transition algorithm require conditional object writes and opaque version
tokens; they do not require an S3-specific artifact or transition.

## Decision

Arco adopts an Arco-owned compare-and-swap state store as the logical authority,
with S3 as the first GA-qualified adapter. A successful conditional replacement
of an authority root's `head/current.json` is the commit point. The resulting
opaque `StateToken` identifies the committed logical sequence and immutable
authority manifest. Provider selection is deployment composition and is not
part of `StateToken`, `control/v1`, or transaction identity.

This ADR fixes the following invariants.

1. There is one metastore authority root per `(tenant_id, workspace_id)`, one
   root per managed Delta table, and separate roots for Flow, lineage, and
   projection acknowledgements. No operation claims atomicity across roots.
2. Canonical state lives beneath a fresh `control/v1/` prefix. Old control
   layouts are neither read nor migrated by this release.
3. Transaction envelopes, manifests, checkpoints, and the mutable head are
   small JSON documents. Sorted key/value changes and consolidated state are
   Arrow IPC segments with checksummed JSON indexes. Before Arrow decoding,
   the kernel validates the checksum-bound footer, exact schema, supported
   metadata version and feature set, record-batch count, block arithmetic,
   stored offsets, and index bounds. Parquet is projection only.
4. A commit writes immutable transaction and segment artifacts, writes an
   immutable candidate manifest, and conditionally replaces the head. The
   state-store kernel performs one CAS and returns a typed conflict to a loser;
   the production API retry layer must re-read authority, re-evaluate every
   precondition, and retry with jitter within the 1.5-second budget. Exhaustion
   is a retryable authority conflict, never a partial success. Every head
   writer reconciles transport ambiguity: normal commits accept exact pointer
   bytes or an exact transaction reference in newer visible lineage, restore
   publication re-inspects the deterministic candidate, and writer-authority
   claims adopt only exact claimed bytes. An outcome that still cannot be
   proven committed or uncommitted returns `AmbiguousAuthorityOutcome`.
5. A committed mutation returns `CommitOutcome { state_token,
   projection_intents }`. Post-commit delivery is best effort and cannot roll
   back or change the successful authority result.
6. Projection and layout-maintenance intents are versioned envelopes.
   Projection watermarks live in a separate authority root. Segment
   consolidation may advance layout generation but never logical sequence.
7. Authorization, object existence, credential vending, and managed Delta
   commit validation read authority state, not Parquet projections. A caller's
   `StateToken` pins catalog state but never grants authorization.
8. The former catalog compactor, its synchronous client/configuration, and the
   public gRPC listener are removed at the catalog cutover. Flow may retain
   deterministic folding, but that operation is a projection and is not named
   or treated as logical catalog compaction.
9. A required L1 replay anchor is rendered before any candidate transaction,
   segment, manifest, or head object is published. Row, byte, or index overflow
   returns `MaintenanceBackpressure`; the kernel does not skip the anchor or
   permit an unbounded replay suffix.
10. Current restore plans persist the positive checkpoint interval used to
    decide and render their replay anchor. Inspection and application use that
    durable value, not the receiving process's current configuration. Retired
    v1/v2 plans remain supersession-only, and a non-`control/v1` authority
    reference returns `UnsupportedAuthorityFormat` with hard-cut recovery
    direction.

### Layout

Each root uses the following versioned shape:

```text
control/v1/domains/{domain}/
  head/current.json
  transactions/{id}.json
  manifests/{id}.json
  segments/l0/{id}.arrow
  segments/l1/{id}.arrow
  indexes/{segment-id}.idx
  checkpoints/{id}.json
```

The head is the only mutable object and may only be written with the selected
provider's exact-version conditional precondition. Segment rows carry sorted binary key/value data,
generation, tombstone, logical sequence, and the logical ordinal needed for
ordered records. Transaction JSON contains metadata and the immutable L0
reference; mutation and outbox payloads live only in the Arrow segment.
Indexes bind the segment checksum and record key bounds, actual Arrow
record-batch offsets, Bloom data, and row counts.

The current kernel validates and replays a bounded manifest suffix from its
Arrow segments. It does not yet use index ranges and Bloom data to avoid full
segment reads for point and prefix lookups. Index-pruned reads, paginated scans,
the API-level 1.5-second CAS retry loop, retention/GC, and real-S3 performance
qualification are cutover requirements rather than claims of this revision.
JSON artifact byte caps, consolidation, and retention/GC also remain cutover
work; typed L1 backpressure is the bounded fail-closed behavior at the current
capacity ceiling.

### Cutover and qualification

This is a hard cut, not a dual-write migration. The old ledger and synchronous
compactor remain the current catalog runtime until native catalog routes are
explicitly switched to the new root. Cutover is forbidden until real-S3
qualification demonstrates conditional-put semantics and the stated latency,
throughput, corruption, recovery, retention, and maintenance gates.
The provider adapters use a distinct single-attempt client for conditional
writes so ambiguous transport failures reach the kernel's reconciliation path,
while safe reads and legacy operations retain the upstream bounded retry policy.
Any future provider-internal conditional retry mode, plus production HTTP
error-envelope mapping for the new kernel errors, must be qualified during
route cutover; they are not established by this repository-only remediation.

### Storage ownership

The `control/v1` kernel depends on a narrow `ScopedAuthorityStore` capability:
scope-relative reads, metadata reads, create-if-absent writes, and exact-version
replacement. It cannot list, delete, sign, or write unconditionally through
that capability. `arco-core` owns the provider-neutral `StorageBackend`
contract and deterministic `MemoryBackend`; it does not construct cloud
clients or select a provider.

`arco-storage-object-store` owns common protocol translation. Provider-specific
builders, credential discovery, capability decisions, and credentialed live
conformance entry points are owned independently by `arco-storage-s3`,
`arco-storage-gcs`, and `arco-storage-azure`. The `arco-storage` composition
crate alone maps deployment bucket references to those adapters. Passing local
or repository conformance does not promote any provider: S3, GCS, and Azure
each require independent live evidence, and S3 remains the first GA target.
Shared adapter construction always requires an explicit conditional-write
client; custom providers must configure that client for a single request so a
lost response reaches authority reconciliation. Moving the previously
published adapter out of `arco-core` is therefore a documented Rust 0.3.0
source boundary rather than an implicit 0.2.x compatibility claim.

If a single metastore root cannot sustain 25 qualified mutations per second,
or maintenance cannot remain ahead of writes, implementation stops for a new
ADR. It must not silently add DynamoDB, a resident writer, implicit sharding,
or another state-store dependency.

## Consequences

- Logical mutation success no longer depends on projection publication or a
  synchronous compactor service.
- Ordinary point reads and bounded scans will use index-pruned control
  segments before cutover; this revision supplies and validates the index but
  still replays each selected segment. Parquet stays useful for system-table
  and discovery projections.
- Immutable losing-CAS artifacts and failed post-commit deliveries require
  recovery, anti-entropy, and garbage-collection workers.
- Cross-root workflows are explicit sagas with fences and receipts rather than
  undocumented transactions.
- ADR-018 describes the pre-cutover synchronous catalog path. ADR-032's
  immutable-manifest/CAS primitive remains valid, while this ADR moves the GA
  catalog authority from Parquet snapshots to `ArcoStateStore` state.
- Repository tests and emulators prove only local behavior. Every provider
  promotion requires its own live conditional-write and recovery evidence.
  AWS promotion additionally requires live S3, KMS, SQS, IAM, Access Grants,
  STS, and regional-recovery evidence.
