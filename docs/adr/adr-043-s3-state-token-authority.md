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
without making a projection part of the transaction. Arco's GA target is AWS,
S3, API Gateway, and Lambda, where a resident writer or synchronous compactor
would defeat scale-to-zero operation.

## Decision

Arco adopts an Arco-owned S3 compare-and-swap state store as the GA logical
authority. A successful conditional replacement of an authority root's
`head/current.json` is the commit point. The resulting opaque `StateToken`
identifies the committed logical sequence and immutable authority manifest.

This ADR fixes the following invariants.

1. There is one metastore authority root per `(tenant_id, workspace_id)`, one
   root per managed Delta table, and separate roots for Flow, lineage, and
   projection acknowledgements. No operation claims atomicity across roots.
2. Canonical state lives beneath a fresh `control/v1/` prefix. Old control
   layouts are neither read nor migrated by this release.
3. Transaction envelopes, manifests, checkpoints, and the mutable head are
   small JSON documents. Sorted key/value changes and consolidated state are
   Arrow IPC segments with checksummed JSON indexes. Parquet is projection
   only.
4. A commit writes immutable transaction and segment artifacts, writes an
   immutable candidate manifest, and conditionally replaces the head. The
   state-store kernel performs one CAS and returns a typed conflict to a loser;
   the production API retry layer must re-read authority, re-evaluate every
   precondition, and retry with jitter within the 1.5-second budget. Exhaustion
   is a retryable authority conflict, never a partial success. A transport
   error after the head write is reconciled by comparing the exact canonical
   pointer bytes before the caller is told the outcome is unknown.
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

The head is the only mutable object and may only be written with an S3
conditional precondition. Segment rows carry sorted binary key/value data,
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

### Cutover and qualification

This is a hard cut, not a dual-write migration. The old ledger and synchronous
compactor remain the current catalog runtime until native catalog routes are
explicitly switched to the new root. Cutover is forbidden until real-S3
qualification demonstrates conditional-put semantics and the stated latency,
throughput, corruption, recovery, retention, and maintenance gates.

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
- Repository tests and emulators prove only local behavior. AWS promotion also
  requires live S3, KMS, SQS, IAM, Access Grants, STS, and regional-recovery
  evidence.
