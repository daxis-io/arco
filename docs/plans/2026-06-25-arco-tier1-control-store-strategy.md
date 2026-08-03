# Arco Tier-1 Control Store Strategy

**Date:** 2026-06-25

**Related planning:**

- [Arco Tier-1 Single Authority Vision](2026-06-26-arco-tier1-single-authority-combined-vision.md)
- [Olympia-Inspired Arco Strategy](2026-06-20-olympia-inspired-arco-strategy.md)

## Purpose

This document evaluates whether Arco should replace synchronous Tier-1
Parquet compaction with an object-store-backed transactional control store.

It covers the full design surface discussed so far:

- the current Arco write path and why it is becoming strained;
- the SlateDB ideas worth borrowing;
- the choice between an Arco-native control store and a direct SlateDB
  dependency;
- the benefits, runtime shape, and always-on service implications;
- the proposed control-store API, file layout, transaction model, keyspace,
  compaction model, read tokens, checkpoints, and garbage collection;
- the migration path, risks, and verification plan.

This is a strategy document, not an accepted ADR. It should guide a focused
prototype and a later ADR if the prototype validates the approach.

## Executive Recommendation

Arco should introduce an internal `arco-state` or `arco-control-store`
abstraction before changing the authoritative Tier-1 write path.

The recommended path is:

1. Add an Arco state-store trait that captures the operations Tier-1 domains
   need: `get`, `scan_prefix`, `begin_txn`, `put`, `delete`,
   precondition checks, `commit`, `checkpoint`, and state-token reads.
2. Implement the current ledger plus synchronous compactor path behind the
   trait first. That gives Arco a migration seam without changing behavior.
   This adapter is transitional: it lets domain services adopt the trait, run
   shadow comparisons, support rollback during migration, and read retained
   historical snapshots. It is not a permanent production write backend for
   migrated Tier-1 domains.
3. Implement a deterministic reference backend and an Arco-native minimal
   object-store control-store prototype. Use SlateDB only as a reference design
   for WAL, manifest, checkpoint, compaction, and single-writer/multi-reader
   patterns.
4. Keep Arco's event model, deterministic replay contract, Parquet projections,
   system tables, projection watermarks, fenced publication, and object-storage
   authority boundary.
5. Move the compactor out of a domain's synchronous success path only after
   logical sequence assignment, state tokens, idempotency, provider CAS,
   read-after-write, authorization freshness, crash recovery, and projection
   equivalence are proven.

The preferred long-term product shape is:

```text
API/domain service
  -> Arco control-store transaction
  -> authoritative state token returned to caller
  -> async compactor publishes Parquet projections and system tables
```

The control store should serve Tier-1 catalog and governance metadata. It
should not become a general user-data KV database, a replacement for Parquet
system tables, or the first home for high-frequency orchestration telemetry.

This document is approved as prototype strategy only. It does not approve
cutting over catalog DDL authority, moving credential vending to a new state
backend, making all Tier-1 Parquet publication asynchronous, or committing to an
Arco-native segment format before the prototype satisfies the decision criteria.

## Intended Final State

If the prototype satisfies the decision criteria in this document, the intended
final Tier-1 architecture is a single-authority control-store model.

All Tier-1 catalog and governance mutations commit through `ArcoStateTxn`. The
visible success boundary is a fenced control-store manifest pointer CAS plus a
returned `StateToken`. Parquet publication is asynchronous and watermarked.
System tables, audit views, lineage, discovery, and exports read from derived
projections or retained checkpoints.

The current ledger plus synchronous compactor path may remain behind
`ArcoStateStore` only during migration, shadow replay, rollback validation, or
support for retained historical snapshots. It is not a permanent alternative
write authority for migrated Tier-1 domains.

After a domain migrates, old-path writes for that domain must be disabled. After
all targeted Tier-1 domains migrate, `arco-state-current` should leave
production write paths. Snapshot and export compatibility may still read
retained historical artifacts.

## Relationship To Olympia-Inspired Arco Strategy

The Olympia-inspired strategy defines the external contract layer that should
surround the control store: storage-format documentation, reader and projection
contracts, workspace snapshots, rollback and export, durable transaction
handles, retention and GC pins, root/read tokens, and conformance fixtures.

This control-store strategy defines the final Tier-1 mutation authority:
`ArcoStateTxn`, object-store transaction records, folded state, control
manifests, state tokens, checkpoints, provider CAS requirements, failure states,
and projection watermarks.

The combined architecture is:

```text
control-store authority
  + Olympia-inspired snapshots/export/transaction UX/storage contract
  + Parquet system-table projections
```

The umbrella [Arco Tier-1 Single Authority Vision](2026-06-26-arco-tier1-single-authority-combined-vision.md)
captures this combined direction and the draft ADR wording. This document keeps
the narrower technical authority-path design and prototype decision criteria.

The two documents should not imply two permanent Tier-1 write paths. Olympia's
domain-manifest language remains useful as a compatibility and external
contract shape. For migrated Tier-1 domains, the control-store commit is the
mutation authority and Parquet manifests are projection publication artifacts.

## Single-Authority Invariant

For every Tier-1 authority scope, exactly one writer path may accept
mutation-visible commits.

During migration, each scope is in one of these states:

1. `OldAuthority`: the ledger, synchronous compactor, and Parquet/JSON manifest
   pointer are authoritative.
2. `ShadowControlStore`: old authority accepts writes; the control store replays
   and compares but does not publish authority.
3. `ControlStoreAuthority`: the control store accepts writes; old-path writes
   are disabled for that scope; Parquet is an asynchronous projection.
4. `RetiredOldAuthority`: old-authority code has left production write paths;
   retained historical artifacts remain readable through snapshot and export
   compatibility rules.

No scope may be in `OldAuthority` and `ControlStoreAuthority` at the same time.

## Current Arco Baseline

Arco's current authoritative control-plane model uses object storage as the
durable source of truth. Writers append immutable events or artifacts. The
compactor writes immutable state artifacts. A fenced pointer CAS publishes the
visible snapshot.

The current Tier-1 catalog DDL path is documented in
`docs/adr/adr-018-tier1-write-path.md` and tracked in
`docs/guide/src/reference/control-plane-scope.md`:

```text
CatalogWriter
  -> acquire distributed lock
  -> append ledger event
  -> call synchronous compactor
  -> compactor reads explicit event paths
  -> compactor writes Parquet snapshot and manifest
  -> compactor CAS-publishes pointer
  -> API returns visible success
```

The key current invariants are:

- the API writes `ledger/`, `locks/`, and commit records;
- the compactor is the sole writer of Parquet state, snapshots, manifests, and
  published visibility;
- listing is not on the request-time correctness path;
- explicit event paths feed synchronous compaction;
- fencing tokens flow from the lock holder to the compactor;
- readers use published snapshots and pointer-selected manifests.

ADR-018 accepts the cost: DDL waits on compactor availability and Parquet
publication. That made sense when Tier-1 operations were rare and the highest
priority was IAM-enforced compactor ownership of Parquet state files.

The catalog surface has grown since then. Arco now has catalog DDL, UC
compatibility, metastore/governance scaffolding, storage credentials, external
locations, grants/RBAC work, Delta commit coordination, lineage/search
publication, and root-token reads. Some of these domains need fast authoritative
point reads, idempotency records, generation checks, and fail-closed
authorization decisions. Parquet remains excellent for open system-table
queries, but it is not the best hot mutation substrate.

## Current Constraint From ADR-039

`docs/adr/adr-039-catalog-consistency-model.md` already gives Arco a compatible
escape hatch:

> For synchronous mutation APIs, a successful visible response means the
> mutation is reflected in the published snapshot or in a transaction-pinned
> read token returned by the API.

That sentence allows a different success gate:

```text
visible success = committed control-store transaction + returned state token
```

Parquet projection publication can lag if:

- enforcement reads use the control store or compiled state at the required
  token;
- system tables expose watermarks or safe stale-projection errors;
- search, lineage, and discovery remain documented derived surfaces;
- stale or missing enforcement state fails closed.

This matters because the proposed control store does not reject Arco's
consistency model. It uses the token-pinned branch that ADR-039 already
anticipates.

## What SlateDB Demonstrates

SlateDB is useful here as a reference design, even if Arco does not adopt it as
a dependency.

The SlateDB design documents describe the classic LSM structure:

- write-ahead log;
- mutable memtable;
- immutable memtables;
- sorted string tables;
- sorted runs;
- manifest state;
- background compaction.

SlateDB writes those structures to object storage instead of a local disk. Its
object-store layout separates manifest files, WAL files, compaction state,
compacted SSTs, and garbage-collection metadata. Its docs describe each manifest
file as a complete snapshot of database state at the time it was written.

The public docs checked on 2026-06-25 describe SlateDB as an embedded
object-storage storage engine with basic KV operations, range scans, manifest
persistence, compaction, transactions, clones, range deletes, change data
capture, database splitting, and database merging. The docs.rs crate index and
the rustdoc API pages did not show the same visible version during this check;
the rustdoc API pages showed `slatedb` 0.13.1, while the crate index page
showed 0.11.2. Both exposed pages showed a dependency on `object_store
^0.12.3`. Any direct dependency decision must pin and verify the exact crates.io
version before implementation.

SlateDB's release policy is relevant to Arco. The docs.rs crate page says
SlateDB follows Semantic Versioning, targets releases about every two months,
guarantees storage-format compatibility only between adjacent versions, and
does not currently guarantee compile-time API compatibility. That makes a direct
dependency viable for a prototype, but it argues against exposing SlateDB
directly through Arco's domain code.

The core ideas to borrow are:

1. object-store WAL or transaction log;
2. in-process mutable writer state;
3. immutable sorted segment files;
4. manifest-selected database state;
5. single active writer with many readers;
6. compare-and-swap and fencing for head movement;
7. background compaction;
8. checkpoints for stable reads;
9. garbage collection pinned by manifests and checkpoints;
10. a CDC or outbox feed for downstream projection builders.

The recommended design borrows those storage-engine ideas while preserving
Arco's domain events, object IDs, authorization model, projection model, and
storage layout conventions.

## Problem Statement

The current synchronous DDL path couples correctness to Parquet publication:

```text
correctness = ledger append + synchronous compactor + Parquet manifest publish
```

That coupling creates four practical issues.

First, Tier-1 latency inherits compactor tail latency. A create-table request
can fail or time out because Parquet projection publication was slow, even when
the authoritative mutation itself was small.

Second, the compactor becomes part of the availability budget for every
strongly consistent DDL operation. That weakens failure isolation. A projection
pipeline incident can block catalog mutation success.

Third, point reads and precondition checks are awkward. Catalog and governance
mutations need known-key checks such as "table name absent", "grant exists",
"idempotency key unused", "object generation equals N", and "external location
prefix allowed". Parquet snapshots can support those checks, but they optimize
for scans and SQL visibility rather than small transactional reads.

Fourth, richer governance expands Tier-1 state. Grants, principals, storage
credentials, external locations, workspace bindings, credential-vending policy,
idempotency, and audit outbox records all need consistent mutation semantics.
Forcing each small mutation through synchronous Parquet snapshot publication
will make the critical path wider than it needs to be.

## Target Architecture

The target architecture splits authority from publication:

```text
Clients and compatibility APIs
  -> arco-api
  -> Arco domain services
  -> arco-control-store
  -> object storage transaction log, segments, manifests
  -> async compactor
  -> Parquet projections and system tables
```

The control store is the authoritative Tier-1 mutation substrate. It stores
small, strongly consistent catalog and governance state. It commits immutable
events and folded KV state together.

Parquet projections remain the open query surface. System tables continue to
serve catalog, access, storage, audit, lineage, and discovery queries. They
become explicitly derived from control-store events and outbox records.

The compactor keeps two jobs:

1. compact control-store log or small segments into larger sorted segments;
2. project authoritative events into Parquet system tables with watermarks.

This preserves Arco's important property: the compactor stays the sole writer of
Parquet state. It just stops being the synchronous authority for every Tier-1
mutation.

The MVP head-ownership model is:

1. The active control-store writer owns the mutation-visible control root.
2. Control-store compaction publishes layout changes through the active writer.
3. Projection watermarks live in a separate projection root.
4. The projection compactor never gets independent CAS authority over the
   mutation-visible control root.

That keeps one authority on mutation-visible state while letting projection
progress lag or recover without blocking mutation commits. It also prevents
control compaction from racing user mutation publication.

Do not allow arbitrary API processes, control compactors, and projection
compactors to update the same control-store head independently. That would
replace the current compactor critical path with a split-brain publication
problem.

## Provider Contract

Arco control-store correctness depends on object-store semantics that must be
spelled out and tested per backend.

Required primitives:

- create-if-absent for immutable transaction, manifest, checkpoint, and segment
  artifacts;
- conditional replace for the current pointer or equivalent compare-and-swap;
- stable object identity, ETag, generation, or version token for pointer CAS;
- durable read-after-successful-write for addressed objects;
- byte-range reads for segment and object validation;
- checksum, digest, or equivalent corruption detection for every authoritative
  artifact;
- deterministic retry behavior after timeout, duplicate request, and partial
  failure.

The provider adapter should expose this contract:

```rust
#[async_trait]
pub trait ControlStoreObjectProvider {
    async fn put_if_absent(
        &self,
        path: &str,
        bytes: Bytes,
        checksum: Checksum,
    ) -> Result<CreateOutcome>;

    async fn read_pointer(&self, path: &str) -> Result<(PointerValue, VersionToken)>;

    async fn compare_exchange_pointer(
        &self,
        path: &str,
        expected: VersionToken,
        next: PointerValue,
    ) -> Result<SwapOutcome>;

    async fn read_object(
        &self,
        path: &str,
        expected_checksum: Option<Checksum>,
    ) -> Result<Bytes>;

    async fn delete_if_unreferenced(
        &self,
        path: &str,
        condition: DeleteCondition,
    ) -> Result<DeleteOutcome>;
}
```

Each provider implementation must define:

- how it obtains the version token;
- what a timeout means for create and compare-exchange;
- which retries are safe and idempotent;
- whether conditional replace is native or emulated;
- whether emulation is permitted for production;
- how checksums or digests are stored and verified;
- whether provider object versioning is required.

No production backend should be enabled without native or proven-safe
conditional pointer update.

Listing can support repair, migration, audit, and anti-entropy. It must not
become the request-time correctness source.

## Public Contract Boundary

Arco should document three contracts at different depths:

1. Provider contract: public and operator-facing. It defines the object-store
   primitives required for correctness: conditional create, pointer CAS, stable
   version tokens, checksums, addressed read-after-write, retry behavior, and no
   listing for request-time correctness.
2. Authority contract: internal but versioned. It defines control-store
   manifests, transaction records, state tokens, checkpoints, logical
   sequences, failure states, GC, replay equivalence, and migration rules. It
   must support tests, downgrade decisions, and compatibility decisions, but it
   is not a third-party write API.
3. Projection/export contract: external reader-facing. It defines Parquet
   system tables, projection watermarks, workspace snapshots, export manifests,
   relocation rules, and retained read tokens.

External engines should not write or infer mutation authority by editing
control-store files. They should use Arco APIs.

Each supported provider needs a capability matrix before the design leaves
prototype stage:

| Provider | Conditional create | Conditional replace | Addressed read-after-write | Stable version token | Checksum validation |
|---|---|---|---|---|---|
| local test store | required | required | required | required | required |
| S3-compatible | verify | verify | verify | verify | verify |
| GCS | verify | verify | verify | verify | verify |
| Azure Blob | verify | verify | verify | verify | verify |

## Benefit Summary

The main benefit is decoupling Tier-1 mutation authority from public Parquet
publication. Arco can make small authoritative control-plane mutations without
waiting for system-table projection work.

Expected benefits:

- lower DDL latency and fewer tail spikes;
- compactor failures no longer block ordinary Tier-1 mutation success;
- cleaner read-after-write semantics through returned state tokens;
- faster authoritative point lookups for names, object IDs, grants, storage
  governance, and idempotency;
- conflict checks move closer to the write transaction;
- governance and credential-vending routes can fail closed from authoritative
  state instead of depending on lagging projections;
- system tables get a clearer contract as derived, watermarked views;
- API responses can distinguish "committed at logical sequence N" from "system
  tables have projected through logical sequence M";
- replayability improves because each transaction can carry both domain events
  and folded state changes;
- Arco can preserve object-storage authority without running an external
  database service.

Costs:

- Arco owns a real transactional state layer;
- the team must define file formats, manifest rules, checkpoints, GC,
  corruption detection, and compatibility;
- an Arco-native control store keeps the dependency graph clean but makes Arco
  own the transaction and recovery contract;
- a warm writer process is still required while Arco accepts mutations.

## Always-On Service Implications

This design does not require an always-on external database. It does require a
running control-plane writer when the system accepts mutations.

Runtime roles:

| Component | Required to accept writes | Always-on external service | Notes |
|---|---:|---:|---|
| Object storage | yes | no | Durable authority. |
| Arco API/domain service | yes | no | The request entrypoint. |
| Active control-store writer | yes | no | Holds writer lease or fencing epoch. |
| Read replicas | no | no | Optional latency and availability aid. |
| Control-store compactor | no | no | Needed for sustained read performance and GC. |
| Parquet projection compactor | no | no | Needed for fresh system tables. |
| External database | no | no | Not part of this design. |

For low-volume deployments, a serverless writer can acquire a lease, load the
current manifest, replay recent log state, commit the mutation, and exit. That
keeps the "scale to zero" story, but cold writes pay startup and replay cost.

For production latency, Arco should expect one warm writer per active
metastore/workspace/domain shard. If the writer dies, another process takes the
lease, reloads from object storage, and resumes from the visible manifest.

The precise claim should be:

```text
No always-on external database. No synchronous compactor dependency. A live
Arco writer is still required while serving mutations.
```

## Scope

The first control-store scope should be Tier-1 only.

Target Tier-1 domains:

- catalogs;
- schemas;
- tables and table current pointers;
- stable object records;
- name indexes;
- principals and groups;
- grants and ownership;
- storage credentials;
- external locations;
- workspace/metastore bindings;
- idempotency records;
- audit outbox;
- projection outbox;
- credential-vending state.

The first writable prototype should be narrower than that target list:

1. implement the state-store traits with the current backend;
2. implement a deterministic reference backend with failure-model tests;
3. run a shadow backend for catalog and governance replay;
4. compare read-only name indexes, grants, object records, table pointers, and
   projection watermarks;
5. make the first production writable domain projection job checkpoints,
   projection outbox acknowledgements, or non-enforcement watermarks;
6. use a synthetic prototype domain for object-store failure tests where useful;
7. move storage-governance metadata next, without credential vending
   dependency;
8. move idempotency only for operations whose full authoritative mutation is
   also committed in the control store;
9. move grants only after freshness and revocation tests pass;
10. move catalog DDL after the state-store contract proves name, ID, ownership,
   table pointer, outbox, compatibility, and rollback semantics.

Do not use grants as the first writable domain. A write-path bug there becomes
an authorization bug. Do not use catalog DDL as the first writable domain
either; it exercises too many semantics at once.

Do not use generic cross-backend idempotency as the first production writable
domain. If the idempotency record lives in the new backend while the protected
mutation still commits through the old path, retries can split between two
authorities. Move idempotency only when the idempotency record and protected
mutation share the same authority boundary.

Avoid in the first tranche:

- task heartbeats;
- executor telemetry;
- high-volume orchestration event streams;
- metrics;
- raw lineage observations;
- arbitrary user data;
- general SQL over control-store files;
- distributed transactions across independent metastores.

ADR-041 already defines a tiered object-storage event log for orchestration.
That work should stay separate. The control store targets strongly consistent
metadata, not high-frequency append-first runtime events.

## State-Store Abstraction

Arco should introduce a storage abstraction under the catalog/metastore domain
services. Domain routes should not depend on SlateDB APIs or on a specific
custom file layout.

Sketch:

```rust
#[async_trait]
pub trait ArcoStateReader {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<KvPair>>;
    async fn read_at(&self, token: StateToken) -> Result<Box<dyn ArcoStateReader>>;
}

#[async_trait]
pub trait ArcoStateStore: ArcoStateReader {
    async fn begin_txn(&self, opts: TxnOptions) -> Result<Box<dyn ArcoStateTxn>>;
}

#[async_trait]
pub trait ArcoStateAdmin {
    async fn current_state_token(&self) -> Result<StateToken>;
    async fn checkpoint(&self, opts: CheckpointOptions) -> Result<CheckpointToken>;
    async fn compact(&self, opts: CompactionOptions) -> Result<CompactionIntent>;
    async fn gc(&self, opts: GcOptions) -> Result<GcReport>;
}

#[async_trait]
pub trait ArcoStateTxn {
    async fn get(&mut self, key: &[u8]) -> Result<Option<VersionedValue>>;
    async fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<KvPair>>;
    async fn put(&mut self, key: &[u8], value: Bytes) -> Result<()>;
    async fn delete(&mut self, key: &[u8]) -> Result<()>;
    async fn assert_absent(&mut self, key: &[u8]) -> Result<()>;
    async fn assert_generation(&mut self, key: &[u8], generation: u64) -> Result<()>;
    async fn assert_range_empty(&mut self, range: KeyRange) -> Result<()>;
    async fn assert_range_unchanged(
        &mut self,
        range: KeyRange,
        observed_generation: u64,
    ) -> Result<()>;
    async fn read_set(&mut self, keys: &[Key], ranges: &[KeyRange])
        -> Result<PredicateInputSet>;
    async fn assert_inputs_unchanged(&mut self, inputs: PredicateInputSet) -> Result<()>;
    async fn commit(self: Box<Self>) -> Result<StateToken>;
    async fn rollback(self: Box<Self>) -> Result<()>;
}
```

Initial implementations:

| Implementation | Purpose |
|---|---|
| `arco-state-current` | Adapts today's ledger plus synchronous compactor behavior. |
| `arco-state-model` | Deterministic in-memory/reference backend for failure and transaction-model tests. |
| `arco-state-control-mvp` | Arco-native object-store txlog plus manifest pointer with bounded replay and no custom segments. |
| `arco-state-control-segments` | Later Arco-native segment and compaction implementation. |

This abstraction lets Arco validate call-site semantics before committing to a
storage engine. It also prevents compatibility APIs from knowing whether the
backend uses today's compactor, the deterministic model, or the Arco-native
control store.

## Transaction Isolation Contract

The MVP transaction model is single active writer plus serializable commit
semantics at the `ArcoStateStore` layer.

Rules:

1. Every transaction reads from a specific parent snapshot.
2. The active writer serializes commit attempts for a control root.
3. Every point read records the key generation observed.
4. Every range read records a range generation, range fence key, or equivalent
   range witness.
5. Every semantic predicate declares the point keys and ranges it read through a
   `PredicateInputSet`.
6. Commit revalidates the transaction's point, range, and predicate input set
   against the latest parent head before publishing.
7. A failed revalidation returns `conflict` or `precondition_failed`; it does
   not publish a partial transaction.

This contract keeps the MVP boring. Arco should not rely on snapshot isolation
plus caller-discipline for catalog and governance semantics. It can revisit
concurrency after the single-writer serialized model proves correctness.

Point preconditions are not enough for catalog and governance semantics. The
state-store contract needs range and predicate checks for operations such as:

- no normalized table name exists in a schema;
- no child objects remain under a schema before delete;
- no overlapping external location prefix exists;
- an actor has privilege through direct grant or group membership;
- rename does not collide with another normalized alias.

Opaque predicate hashes are not enough. Authorization, path-overlap, and rename
checks must declare the point keys and ranges they observed, then commit must
revalidate those inputs against the latest parent head. External locations need
special care: a lexicographic prefix scan can miss ancestor and descendant
relationships unless Arco stores path-component indexes or explicit ancestor
keys. The storage-governance design should include both forward path keys and
ancestor lookup keys before credential vending depends on it.

## Arco-Native Control Store Layout

If Arco builds the control store itself, use an Arco-native layout rather than
SlateDB's exact paths:

```text
{bucket}/{tenant}/{metastore_or_workspace}/control/
  manifest/
    00000000000000000001.manifest.json
    00000000000000000002.manifest.json
    current.pointer.json
  txlog/
    01J....txn
    01K....txn
    01L....txn
  segments/
    l0/
      01J....segment
    l1/
      01J....segment
    l2/
      01J....segment
  checkpoints/
    chk_01J....json
  gc/
    protected_manifests.json
    protected_segments.json
  projections/
    watermarks/
      catalog.json
      access.json
      storage.json
```

Responsibilities:

| Prefix | Meaning |
|---|---|
| `txlog/` | Immutable transaction records. |
| `segments/` | Sorted KV runs generated from committed transactions. |
| `manifest/` | Complete visible state description and pointer. |
| `checkpoints/` | Pinned read tokens for scans, projection jobs, backup, and tests. |
| `gc/` | Retention metadata and protected references. |
| `projections/watermarks/` | Last projected sequence per derived surface. |

The manifest pointer remains the visibility gate. Immutable transaction objects
exist before publication, but readers treat a transaction as visible only when a
manifest includes it.

## Logical State, Physical Layout, And Head Ownership

The control store must separate logical mutation order from physical layout
generation.

Definitions:

| Term | Meaning |
|---|---|
| `logical_sequence` | Monotonic sequence of visible committed user mutations for a scope. |
| `snapshot_id` or `manifest_id` | Physical manifest/layout identifier that can change during mutation commits or compaction. |
| `layout_generation` | Physical generation for segment organization, compaction output, and manifest shape. |
| `projection_watermark` | Highest logical sequence included in a derived Parquet projection. |

Rules:

1. A visible user mutation advances `logical_sequence`.
2. A compaction job may advance `snapshot_id` or `layout_generation` without
   changing `logical_sequence`.
3. The published manifest assigns visible logical sequences to transaction
   files.
4. A physical transaction file written before pointer CAS has only a physical
   transaction ID and candidate metadata. It has no visible logical sequence.
5. A state token returned to a caller must name both the logical sequence and a
   retained snapshot or manifest that can serve read-after-write.
6. A later manifest that covers the token's `logical_sequence` may satisfy
   `read_at(token)` if the original manifest expired after its retention window.
7. Readers consume transactions reachable from a published manifest. They do
   not list raw `txlog/` paths to discover committed mutations.
8. Only one fenced authority publishes a given control root. Compaction and
   projection watermark updates must route through that authority or use
   separate CAS-protected roots.

Suggested token shape:

```json
{
  "scope": "tenant/acme/metastore/lakehouse_prod",
  "logical_sequence": 18422,
  "snapshot_id": "manifest-01J...",
  "issued_at": "2026-06-25T00:00:00Z",
  "min_retained_until": "2026-06-25T01:00:00Z"
}
```

This distinction prevents compaction from invalidating recently returned write
tokens. It also lets Arco publish new physical manifests for storage layout
maintenance without pretending a user mutation occurred.

## Transaction Record

Every authoritative mutation should produce one durable transaction record.

Example:

```json
{
  "txn_id": "01JZ...",
  "tenant_id": "acme",
  "metastore_id": "lakehouse_prod",
  "workspace_id": "prod",
  "domain": "catalog",
  "physical_txn_id": "txlog/01J....txn",
  "candidate_parent_snapshot": "manifest-01J...",
  "writer_epoch": 17,
  "request_id": "req_...",
  "idempotency_key": "client-key-...",
  "actor": "user:alice@example.com",
  "preconditions": [
    {
      "key": "name/table/schema_123/orders",
      "must_not_exist": true
    },
    {
      "key": "obj/schema_123",
      "generation": 8
    }
  ],
  "writes": [
    {
      "key": "obj/table_456",
      "generation": 1,
      "value_ref": "inline-or-external"
    },
    {
      "key": "name/table/schema_123/orders",
      "value": "table_456"
    },
    {
      "key": "event/catalog/by_txn/01JZ...",
      "value": {
        "type": "TableCreated",
        "table_id": "table_456"
      }
    },
    {
      "key": "outbox/by_txn/01JZ...",
      "value": {
        "projection": "catalog"
      }
    }
  ],
  "deletes": [],
  "state_hash_before": "sha256:...",
  "state_hash_after": "sha256:..."
}
```

The transaction record is the durable mutation record. The manifest makes it
visible. The event keys preserve Arco's event-sourcing and replay contract. The
folded KV writes give fast authoritative reads.

Visible logical sequences should be monotonic and unambiguous. Physical
transaction artifacts may contain gaps because a writer can lose a CAS race
after writing an object. The published manifest owns visible sequence
assignment:

```text
logical_sequence 18422 -> txlog/01J....txn
```

Projection jobs, audit readers, and CDC consumers must read only
manifest-reachable transactions. They must treat raw listed txlog objects as
candidates for repair or audit, not visible data.

Repair jobs must not promote orphan transaction files blindly. A repair process
may publish an orphan only after it revalidates the transaction against the
current head and proves the resulting state is equivalent to a valid retry.

`state_hash_before` and `state_hash_after` should not require hashing the
entire logical database on every write. For the MVP, use a deterministic hash of
the transaction envelope, read/write set, parent manifest ID, and resulting
manifest ID. Full state-root hashing can be a later Merkle-style improvement if
the control store needs stronger global integrity proofs.

## Manifest Model

The control-store manifest should describe the complete visible state root:

```json
{
  "snapshot_id": "manifest-01J...",
  "layout_generation": 57,
  "writer_epoch": 17,
  "highest_logical_sequence": 18422,
  "visible_transactions": [
    {
      "logical_sequence": 18419,
      "txn_path": "txlog/01J...A.txn"
    },
    {
      "logical_sequence": 18420,
      "txn_path": "txlog/01J...B.txn"
    },
    {
      "logical_sequence": 18421,
      "txn_path": "txlog/01J...C.txn"
    },
    {
      "logical_sequence": 18422,
      "txn_path": "txlog/01J...D.txn"
    }
  ],
  "valid_txlog": [
    "txlog/01J...A.txn",
    "txlog/01J...B.txn",
    "txlog/01J...C.txn",
    "txlog/01J...D.txn"
  ],
  "segments": [
    {
      "level": 0,
      "path": "segments/l0/01J....segment",
      "min_key": "grant/object/...",
      "max_key": "table/current/..."
    },
    {
      "level": 1,
      "path": "segments/l1/01J....segment",
      "min_key": "name/catalog/...",
      "max_key": "obj/..."
    }
  ],
  "projection_watermarks": {
    "catalog": 18410,
    "access": 18405,
    "storage": 18401
  },
  "created_at": "2026-06-25T00:00:00Z"
}
```

The MVP should keep `valid_txlog` bounded. It should include only recent,
uncompacted, still-visible transaction files. Older transactions should be
represented by segments plus retention metadata. The full immutable event
archive can remain queryable through event keys, audit projections, or explicit
archive paths.

Publish protocol:

1. Writer reads `current.pointer.json`.
2. Writer loads the current immutable manifest.
3. Writer evaluates preconditions and builds a transaction record.
4. Writer writes the transaction record with `DoesNotExist`.
5. Writer writes the next immutable manifest with `DoesNotExist`.
6. Writer CAS-updates `current.pointer.json` from expected pointer version to
   the new manifest.
7. Writer updates its in-memory memtable only after pointer CAS succeeds.
8. Writer returns `StateToken { scope, logical_sequence, snapshot_id,
   issued_at, min_retained_until }`.

If pointer CAS fails, the transaction file and manifest are orphan candidates.
They are not visible. A later repair process may reuse their payload only after
it revalidates preconditions against the current head or proves the retry is
equivalent.

## KV Keyspace

The control store should use typed lexicographic keys. Range scans must map to
real domain access patterns.

```text
# Object identity
obj/catalog/{catalog_id}
obj/schema/{schema_id}
obj/table/{table_id}
obj/view/{view_id}
obj/volume/{volume_id}
obj/function/{function_id}
obj/model/{model_id}

# Name indexes
name/catalog/{catalog_name}
name/schema/{catalog_id}/{schema_name}
name/table/{schema_id}/{table_name}
name/view/{schema_id}/{view_name}

# Table state
table/{table_id}/current
table/{table_id}/version/{version}
table/{table_id}/commit/{commit_id}

# Access control
principal/{principal_id}
group/{group_id}/member/{principal_id}
grant/object/{object_id}/{principal_id}/{privilege}
grant/principal/{principal_id}/{object_id}/{privilege}

# Storage governance
storage_credential/{credential_id}
external_location/{location_id}
location/by_prefix/{encoded_path}/{location_id}
location/by_component/{component_hash_path}/{location_id}
location/ancestor/{encoded_path}/{ancestor_location_id}
workspace_binding/{workspace_id}/{metastore_id}

# Tags and policies
tag/{tag_name}/{object_id}
policy/{policy_id}
policy_binding/{object_id}/{policy_id}

# Idempotency
idempotency/{client_id}/{idempotency_key}

# Immutable events
event/catalog/{sequence}
event/access/{sequence}
event/storage/{sequence}

# Projection outbox
outbox/{sequence}
```

Range examples:

```text
list tables in schema:
  scan name/table/{schema_id}/

list grants on object:
  scan grant/object/{object_id}/

list grants for principal:
  scan grant/principal/{principal_id}/

find external locations under prefix:
  scan location/by_prefix/{encoded_path_prefix}/

check ancestor and descendant path conflicts:
  scan location/by_component/{component_hash_path}/
  scan location/ancestor/{encoded_path}/
```

Keys must include version and scope prefixes from the first prototype:

```text
v1/t/{tenant_id}/m/{metastore_id}/...
```

That makes compatibility and migration explicit. Changing the prefix later
would create avoidable migration work and make early object-store evidence less
useful.

Key rules:

- store normalized names in index keys and preserve display names in object
  values;
- define case sensitivity per object family before implementation;
- apply one Unicode normalization form before key encoding;
- escape or reject separator characters that conflict with key layout;
- define maximum key length and maximum encoded path depth;
- include tenant and metastore scope in every authoritative key;
- generate stable object IDs outside mutable names;
- represent deletes with tombstones until retention and replay rules permit
  physical cleanup.

## Segment Format

The first Arco-native writable prototype should not start with a custom segment
format. It should use:

```text
physical txlog objects
published manifest pointer
bounded manifest-reachable replay
projection checkpoints and watermarks
```

That tests the authority, token, provider-CAS, and failure semantics without
turning the first milestone into a storage-engine format project.

If bounded replay works and read amplification becomes the next bottleneck,
Arco can add native segments. Three internal segment formats are plausible.

### Option A: Custom Binary Segments

Use a compact LSM-style file:

```text
header
block index
bloom filters
compressed key-value blocks
footer
```

Benefits:

- best point-lookup and range-scan shape;
- small overhead;
- room for prefix bloom filters and custom integrity checks.

Costs:

- Arco owns a storage-engine file format;
- debugging requires custom tools;
- compatibility work starts immediately.

### Option B: Arrow IPC Segments

Use Arrow IPC for sorted KV rows:

```text
key: binary
generation: uint64
tombstone: bool
value: binary
value_schema: string
txn_sequence: uint64
```

Benefits:

- more Arco-native than a custom binary format;
- easier to inspect with existing Arrow tooling;
- simpler bridge to Parquet projection builders.

Costs:

- point lookups need side indexes or small segments;
- bloom filters and block indexes require additional metadata;
- Arrow IPC alone does not solve read amplification.

### Option C: Parquet Segments

Use Parquet for internal control-store segments.

Benefits:

- easiest to inspect and query;
- aligns with Arco's public projection story.

Costs:

- weaker hot point-read shape;
- higher startup and small-read overhead;
- risks recreating the current design tension by asking Parquet to serve both
  the mutation store and the public query surface.

Recommendation for an Arco-native engine: start with Arrow IPC or a small
custom binary format for control-store segments. Keep Parquet for public
projections and system tables. Do not start with Parquet segments for the hot
control-store path unless the prototype proves point-read latency and read
amplification stay within budget.

## Write Path

Create-table should become:

```text
1. API receives create_table.
2. API routes to active control-store writer.
3. Writer loads the latest manifest.
4. Writer reads from memtable, recent txlog, and compacted segments.
5. Writer validates catalog, schema, normalized name absence, grants, storage
   policy, idempotency, and any range or predicate preconditions.
6. Writer writes one transaction object.
7. Writer writes the next manifest.
8. Writer CAS-publishes the manifest pointer.
9. Writer updates its in-memory memtable.
10. API returns table_id and state_token.
11. Compaction later writes control segments through the chosen head-ownership
    model and publishes Parquet projections asynchronously.
```

Pseudo-code:

```rust
async fn create_table(
    req: CreateTableRequest,
    ctx: RequestContext,
) -> Result<CreateTableResponse> {
    let mut txn = state.begin_txn(TxnOptions::serializable()).await?;

    if let Some(saved) = lookup_idempotency(&mut txn, &ctx).await? {
        return Ok(saved.deserialize()?);
    }

    let catalog_id = resolve_catalog(&mut txn, &req.catalog).await?;
    let schema_id = resolve_schema(&mut txn, catalog_id, &req.schema).await?;
    authorize(&mut txn, &ctx.actor, Action::CreateTable, schema_id).await?;

    let normalized_name = normalize_table_name(&req.name)?;
    let name_key = key::table_name(schema_id, &normalized_name);
    txn.assert_absent(&name_key).await?;
    txn.assert_range_empty(key::table_alias_range(schema_id, &normalized_name)).await?;

    let physical_txn_id = PhysicalTxnId::new();
    let table_id = ObjectId::new_table();

    txn.put(
        key::event_by_txn("catalog", physical_txn_id),
        encode(&CatalogEvent::TableCreated {
            physical_txn_id,
            table_id,
            schema_id,
            name: normalized_name.clone(),
            actor: ctx.actor.clone(),
            request_id: ctx.request_id.clone(),
        })?,
    ).await?;

    txn.put(
        key::object(table_id),
        encode(&TableObject::from_request(&req, table_id, schema_id))?,
    ).await?;
    txn.put(name_key, encode(&table_id)?).await?;
    txn.put(key::table_current(table_id), encode(&initial_current(&req))?).await?;
    txn.put(key::owner_grant(table_id, &ctx.actor), encode(&Grant::owner())?).await?;
    txn.put(
        key::outbox_by_txn(physical_txn_id),
        encode(&ProjectionEvent::from_catalog_txn(physical_txn_id))?,
    ).await?;
    txn.put(key::idempotency(&ctx), encode(&IdempotencyResult::success(table_id))?).await?;

    let token = txn.commit().await?;

    Ok(CreateTableResponse {
        table_id,
        state_token: token,
    })
}
```

No Parquet snapshot is required before returning success.

`commit()` assigns the visible logical sequence by publishing a manifest entry
that maps the physical transaction ID to the next logical sequence. The
pre-publish transaction file does not own that sequence.

## Read Behavior

The design needs three read modes.

| Read mode | Source | Consistency |
|---|---|---|
| Control-plane read | control store | current or token-pinned authoritative state |
| Enforcement read | control store or compiled state from same token | fail closed if stale or missing |
| System-table read | Parquet projection | watermarked, may lag |

API responses should expose both the authoritative token and projection
freshness:

```json
{
  "table_id": "table_456",
  "state_token": {
    "scope": "tenant/acme/metastore/lakehouse_prod",
    "logical_sequence": 18422,
    "snapshot_id": "manifest-01J...",
    "issued_at": "2026-06-25T00:00:00Z",
    "min_retained_until": "2026-06-25T01:00:00Z"
  },
  "projection_status": {
    "catalog_watermark": 18410,
    "system_tables_may_lag": true
  }
}
```

Rules:

- catalog `get` and `list` endpoints read authoritative control-store state;
- authorization and credential vending read authoritative state or compiled
  state that is at least as fresh as the required token;
- stale enforcement state denies closed;
- `system.*` SQL reads Parquet projections and exposes watermarks;
- search, lineage, and discovery read derived projections and expose freshness.

`read_at(token)` should first try the token's referenced snapshot. If that
snapshot is unavailable after the token retention window, the reader may use a
later manifest that covers `token.logical_sequence`. If neither exists, the
route returns `TokenExpired` or a documented stale-state error. It must not
silently fall back to an older projection.

## Enforcement And Credential Freshness

Authorization and credential vending are enforcement paths. They must not read
from Parquet system-table projections.

They may read only:

1. current authoritative control-store state;
2. a compiled authorization or storage-governance cache whose freshness token is
   greater than or equal to the required token;
3. a deliberately stale cache only for operations whose route contract marks
   stale-allow as safe.

Privilege revocation needs explicit behavior:

- revocation commits advance the authoritative logical sequence;
- credential vending after revocation must read state at or after the
  revocation sequence or deny closed;
- compiled authorization caches must carry a freshness token and a revocation
  watermark;
- cache invalidation failures must degrade to deny, not stale allow;
- control-store unavailability during credential vending should return an
  auditable deny or unavailable response, not mint broad credentials.

The ADR should define a revocation freshness budget before moving grants,
compiled permissions, or credential vending onto the new backend.

## Writer Fencing

The current distributed lock concept still applies, but it protects the
control-store writer instead of the ledger-to-compactor critical path.

Each transaction and manifest includes:

```json
{
  "writer_epoch": 17
}
```

Publish is allowed only when:

```text
current_pointer.version == expected_pointer_version
current_manifest.writer_epoch <= writer_epoch
```

If a writer sees a higher epoch, it stops acknowledging writes. If the writer
loses the CAS race, it discards or retries the transaction after re-reading the
new head.

The writer lease covers the control root for its scope. Any component that wants
to publish compaction output or projection-watermark changes into that root must
either:

- submit an intent to the active writer;
- become the active writer under the same fencing protocol;
- write to a separate CAS-protected root whose ownership rules are independent.

The important invariant is unchanged from Arco's current pointer model:

```text
immutable artifacts are written first; a fenced pointer CAS makes them visible.
```

## Checkpoints And State Tokens

A checkpoint pins a snapshot or manifest plus its covered logical sequence:

```json
{
  "checkpoint_id": "chk_01J...",
  "snapshot_id": "manifest-01J...",
  "logical_sequence": 18422,
  "created_by": "projection/catalog",
  "expires_at": "2026-06-26T00:00:00Z"
}
```

Use checkpoints for:

- long-running admin scans;
- projection jobs;
- replay-equivalence tests;
- backup and export;
- migrations;
- debug and audit reads;
- token-pinned API flows.

GC must not delete files referenced by an active checkpoint. Expired checkpoints
can release manifests, segments, and transaction files that no visible state or
other checkpoint still references.

State tokens can be lighter than named checkpoints. A state token returned from
a write identifies the logical sequence and a retained snapshot. Arco must keep
that token usable for at least `min_retained_until`. A named checkpoint adds
explicit retention guarantees for longer operations.

Token contract:

| Token | Retention rule | Expiry behavior |
|---|---|---|
| Mutation `StateToken` | Valid for read-after-write until `min_retained_until`. | Return `TokenExpired` or use a later manifest that covers the logical sequence. |
| Named `CheckpointToken` | Pins referenced files until expiration or release. | Return `CheckpointExpired`; do not serve stale data. |
| Projection checkpoint | Projection job must renew before expiration. | Projection fails safely and restarts from a retained checkpoint or watermark. |

## Workspace Snapshots And Export Integration

A workspace snapshot pins control-store authority, projection progress, and
event/archive boundaries.

Example:

```text
WorkspaceSnapshot {
  snapshot_id
  scope
  created_at
  retained_until
  authority_heads: {
    catalog: ControlStoreCheckpointRef {
      control_root
      manifest_id
      logical_sequence
      checkpoint_id?
      manifest_hash
      min_retained_until
    }
    access: ControlStoreCheckpointRef { ... }
    storage: ControlStoreCheckpointRef { ... }
  }
  projection_watermarks: {
    system.catalog: 18410
    system.access: 18405
    system.storage: 18401
    system.audit: 18390
  }
  event_archive_boundaries: {
    catalog: [1, 18422]
    access: [1, 18405]
    storage: [1, 18401]
  }
  export_policy
}
```

Export manifests must include every object needed to restore or audit the
snapshot: control manifests, retained transaction files, segments, checkpoints,
domain event archive objects, projection manifests, Parquet projection files,
watermarks, checksums, layout versions, and relocation metadata.

Restore is roll-forward. It publishes a new visible authority state or
root/read token that references the retained historical cut. It does not mutate
old snapshots. Snapshot and export roots are retained cuts, not competing
mutation authorities.

## Compaction

The compactor should have two independent jobs, but not uncontrolled write
authority over the same head.

Control-store compaction:

```text
txlog + L0 segments
  -> sorted L1/L2 segments
  -> compaction intent or writer-owned layout update
  -> old txlog retained until unpinned
```

Projection compaction:

```text
event/outbox records
  -> Parquet system tables
  -> projection manifests
  -> projection watermarks
```

The projection compactor consumes either explicit outbox keys or a CDC stream.
For Arco, explicit outbox records are the safer first contract because they are
domain events, not storage-engine implementation details.

Head-update rules:

- if the active writer owns the control root, control-store compaction publishes
  through the active writer or runs inside that writer process;
- projection watermarks live in a separate projection root with its own CAS,
  fencing, and failure-state table;
- the projection compactor updates the projection root, not the mutation-visible
  control root;
- projection jobs consume only manifest-reachable transactions or outbox
  records, not raw `txlog/` listings.

Output examples:

```text
system.catalog.catalogs
system.catalog.schemas
system.catalog.tables
system.catalog.columns
system.catalog.table_versions
system.access.grants
system.access.compiled_permissions
system.storage.external_locations
system.audit.events
system.lineage.edges
```

Projection watermarks live in a separate projection root for the MVP. Projection
metadata may copy the watermark for reader convenience, but the projection root
is the publication authority for projection progress. A dual-write design needs
an explicit recovery rule before use.

```text
projection/catalog/watermark -> 18410
projection/access/watermark  -> 18405
projection/storage/watermark -> 18401
```

The compactor can be down while DDL succeeds. It must catch up before system
tables, search, lineage, audit views, or projected enforcement caches can claim
freshness.

## Event-Sourcing Contract

The control store must not turn Arco into "just a KV store."

Every mutation still writes immutable events:

```text
event/catalog/{sequence}
event/access/{sequence}
event/storage/{sequence}
```

The difference is that Arco commits the event and folded state together:

```text
transaction:
  put event/catalog/18422
  put obj/table_456
  put name/table/schema_123/orders
  put table/table_456/current
  put grant/object/table_456/alice/OWNER
  put outbox/18422
```

Replay over the authoritative event stream must yield the same typed state as
the folded KV view at the corresponding state token. That replay-equivalence
test should be a release gate for any domain moved into the control store.

## Txlog, Event Archive, And Retention

The strategy needs to separate storage-engine logs from Arco's immutable domain
event contract.

| Artifact | Purpose | Retention |
|---|---|---|
| Control txlog | Operational commit substrate for the control store. | Short-to-medium retention; compactable after manifest, segment, token, and checkpoint rules allow it. |
| Domain event archive | Immutable audit and deterministic replay stream. | Governance and audit policy; not removed merely because the control txlog compacted. |
| Parquet audit/system projections | Queryable derived views. | Query and history policy; rebuildable from the domain event archive. |
| Control segments | Folded state for efficient reads. | Compactable and GC-able when unpinned. |
| Manifests and checkpoints | Visibility, read tokens, and retention pins. | Retained by read-after-write, migration, backup, and checkpoint policy. |

This separation prevents a future conflict between "events are immutable" and
"txlog can be garbage-collected." The control txlog is an operational storage
engine artifact. The domain event archive is Arco's replay and audit contract.

## API Impact

Compatibility APIs should not change their public semantics.

```text
Iceberg REST API
  -> arco-catalog domain service
  -> arco-state trait

UC-like API
  -> arco-catalog domain service
  -> arco-state trait

SQL system catalog
  -> Parquet projections
  -> visible projection watermark
```

Internal API changes:

- mutation responses include a state token;
- routes that need read-after-write can pass a token into follow-up reads;
- authorization and credential vending take required freshness as input;
- system-table responses expose projection watermarks or stale-projection
  errors where applicable.

State token exposure should vary by API surface:

| Surface | Token behavior |
|---|---|
| Arco-native APIs | May return state tokens in response bodies and metadata. |
| Internal Arco services | Pass full internal tokens between API, enforcement, compactor, and tests. |
| Iceberg REST compatibility | Prefer response headers or internal-only handling unless the client/spec tolerance for extension fields is verified. |
| UC-like compatibility | Prefer optional headers or extension fields only where tolerated. |
| SQL system tables | Expose projection watermarks, not authoritative control-store tokens. |

Candidate headers:

```text
Arco-State-Token: ...
Arco-Projection-Watermark: catalog=18410
```

Do not change Iceberg or UC-compatible response bodies until client
compatibility is verified.

The public framing should change from:

```text
Parquet snapshots are the synchronous source of truth for online mutations.
```

to:

```text
Arco uses object-storage-backed transactional control state for authoritative
Tier-1 mutations and publishes open Parquet projections for SQL, audit,
lineage, and discovery.
```

That is still file-native and object-store-native. It is less pure than
"Parquet is the online mutation substrate," but it fits a production catalog
and governance control plane better.

## Arco-Native Versus Direct SlateDB

### Option 1: Arco-Native Minimal Control Store

Arco implements the minimal object-store transaction layer needed to validate
the authority model:

```text
physical txlog objects
manifest pointer
state tokens
projection checkpoint/watermark root
bounded replay
failure-model tests
```

Benefits:

- exact domain semantics;
- storage layout stays Arco-owned;
- no dependency on a young storage engine's API stability;
- direct integration with Arco events, watermarks, object IDs, and IAM model.

Costs:

- Arco owns the transaction contract;
- correctness burden includes manifests, fencing, recovery, checkpoints, GC,
  and compatibility;
- native segments and compaction are still later work.

Use this for the first writable prototype. Do not design a custom segment
format until the authority, token, provider-CAS, and projection model works.

### Option 2: SlateDB As Reference Design

Arco uses SlateDB's public design as a reference for object-store WAL,
manifests, checkpoints, single-writer/multi-reader behavior, compaction, and
GC.

SlateDB is prior art for object-store LSM design. It is not the architecture's
unit of ownership. The unit of ownership is the Arco control-store contract:
state tokens, control manifests, immutable transaction records, checkpoints,
provider CAS, replay equivalence, and Parquet projection watermarks.

Benefits:

- keeps the strategy grounded in a real object-store LSM design;
- avoids direct dependency, API-compatibility, and storage-format risk;
- lets Arco keep its storage contract Arco-native from the first prototype.

Costs:

- Arco does not get a production LSM implementation for free;
- Arco must own the file layout, manifest rules, crash recovery, and tests.

This is the recommended SlateDB posture for this strategy: borrow the design,
not the dependency.

### Option 3: Direct SlateDB Dependency

Arco embeds SlateDB behind an adapter.

Benefits:

- fastest way to experiment with an existing object-store LSM implementation;
- useful as a throwaway local comparison if dependency friction is acceptable.

Costs:

- current Arco uses `object_store = 0.11`; the checked SlateDB docs.rs pages
  show `object_store ^0.12.3`, so the dependency graph needs an audit;
- SlateDB's public crate page does not promise compile-time API compatibility;
- SlateDB storage-format compatibility policy is not Arco's public storage
  contract;
- Arco still has to wrap events, idempotency, projection outbox,
  authorization, and state tokens.

Do not make this the recommended prototype path.

### Fallback: Keep Current Path And Add Derived Indexes

Arco keeps synchronous compaction as the Tier-1 success gate and adds
manifest-referenced point-lookup indexes.

This is not the preferred final architecture if the control-store prototype
meets its decision criteria. It remains the fallback if the prototype fails to
prove correctness, latency, operational simplicity, provider CAS safety,
authorization freshness, or projection equivalence.

Benefits:

- least architecture churn;
- preserves accepted ADR-018 behavior;
- improves read path without changing write authority.

Costs:

- DDL success still waits on the compactor;
- derived indexes do not solve idempotency and conflict checks as cleanly;
- governance mutation growth still widens the synchronous compaction path.

### Recommendation

Build the abstraction first. Then run prototypes in this order:

1. `arco-state-current` as the compatibility adapter for today's path.
2. `arco-state-model` as the deterministic reference backend.
3. `arco-state-control-mvp` as the Arco-native object-store txlog plus manifest
   pointer implementation.
4. `arco-state-control-segments` only after bounded replay and state tokens
   prove the authority model.

Use SlateDB's design as prior art. Do not introduce SlateDB as a dependency for
the recommended prototype path.

## IAM And Security Boundaries

The current capability split still matters.

Target capabilities:

| Component | Required access |
|---|---|
| API/control writer | control-store txlog, control-root pointer CAS, lock/lease paths |
| Control-store compactor | read control state, write compaction candidates, submit writer intents or hold writer lease |
| Projection compactor | read control events/outbox, write Parquet projections and projection manifests, update projection watermark through chosen root |
| Query service | read Parquet projections and projection manifests |
| Enforcement service | read authoritative control state or compiled state at required token |
| Engines | no direct control-store access; use Arco APIs for metadata and credentials |

No two roles get independent CAS authority over the same mutation-visible root.

Root ownership:

| Root | Owner | Purpose |
|---|---|---|
| Control root | Active control-store writer | Tier-1 mutation visibility. |
| Control layout update | Active writer or writer-mediated compaction | Segment/log compaction visibility. |
| Projection root | Projection compactor | Parquet system-table projection visibility. |
| Snapshot root | Snapshot service | Immutable retained cuts. |
| Export root | Export service | Portable package manifests. |

The compactor remains the sole writer of Parquet state. The API becomes a writer
of authoritative control-store state, not public Parquet projections.

Credential vending must read a fresh-enough authoritative state token or deny
closed. It must not trust lagging system tables.

If a control-store compactor needs to publish layout changes into the same root
as user mutations, it must either run under the active writer lease or publish
through the active writer. IAM should reflect that rule; do not grant a separate
role broad, independent pointer-CAS authority over the same root.

## Migration Plan

### Phase 0: Design And Test Contract

- Write an ADR after this strategy is reviewed.
- Define `StateToken`, `CheckpointToken`, `TxnOptions`, `VersionedValue`,
  `ArcoStateReader`, `ArcoStateStore`, `ArcoStateTxn`, and `ArcoStateAdmin`.
- Define required semantics for logical sequences, layout generations,
  idempotency, conflict errors, read tokens, token retention, stale reads,
  range preconditions, provider CAS, and projection watermarks.

### Phase 1: Trait With Current Backend

- Implement `arco-state-current` over today's ledger plus synchronous compactor.
- Keep DDL behavior unchanged.
- Make domain services call the trait for new code paths.
- Prove no external API behavior changes.
- Treat `arco-state-current` as a migration adapter, not a permanent peer
  backend for migrated Tier-1 domains.

Removal gate: once catalog/governance domains have migrated and retained
snapshot/export compatibility is handled, production Tier-1 mutations should no
longer route through `arco-state-current`.

### Phase 2: Reference And MVP Backends

- Implement `arco-state-model` as a deterministic in-memory/reference backend.
- Implement `arco-state-control-mvp` with object-store txlog objects,
  manifest pointer publication, state tokens, and bounded replay.
- Do not implement custom segments in this phase.
- Run failure-model tests against both backends.

### Phase 3: Shadow Backend

- Import current published catalog/governance state into the new backend.
- Replay existing ledgers into events and folded state.
- Compare object counts, name indexes, grants, table pointers, watermarks, and
  deterministic replay hashes.
- Do not accept writes in the shadow backend.

### Phase 4: Internal Reads From New Backend

- Route selected internal control-plane comparison reads to the new backend.
- Keep current synchronous compaction as the write authority.
- Compare name indexes, grants, object records, table pointers, and projection
  watermarks against published snapshots.
- Keep enforcement on the current path until freshness and revocation tests
  pass.

### Phase 5: First Writable Domain

- Move projection job checkpoints, projection outbox acknowledgements, or
  non-enforcement watermarks first.
- Use a synthetic internal domain for production object-store failure tests if
  needed.
- Projection jobs consume only manifest-reachable outbox records.

### Phase 6: Storage-Governance Metadata

- Move storage credentials, external locations, and path-governance metadata
  without depending on credential vending at first.
- Prove ancestor/descendant path conflict checks, range predicates, and
  projection lag behavior.
- Keep credential vending on the current path until revocation freshness and
  deny-closed tests pass.

### Phase 7: Idempotency, Grants, Or Catalog DDL Pilot

- Move idempotency only for operations whose full authoritative mutation also
  commits in the control store.
- Move grants only after freshness, revocation, and compiled-cache tests pass.
- Move a catalog DDL subset only after name, ID, ownership, table pointer,
  outbox, compatibility, rollback, and projection tests pass.
- Return state tokens for successful writes and expose projection watermarks.

### Phase 8: Remove Synchronous Compaction From A Domain

- Remove synchronous compaction only for the domain that passed replay
  equivalence, failure-state, provider-CAS, and stale-enforcement tests.
- Keep rollback to the old backend until the new domain has production evidence.
- Disable old-path writes for that authority scope before accepting
  control-store writes as production authority.

### Phase 9: Expand To Full Catalog DDL

- Move catalogs, schemas, tables, table current pointers, and name indexes.
- Preserve stable object IDs.
- Preserve rename semantics where grants, lineage bindings, storage bindings,
  and audit identity stay attached to object IDs.
- Preserve Iceberg/Delta commit precondition behavior.

### Phase 10: Retire Old Ledger Authority Per Domain

- Keep an immutable exported event archive for audit and replay.
- Remove synchronous compaction as a success gate only after the domain's
  projected system tables and enforcement paths prove the new model.
- Remove old-authority production write routing for migrated domains. Retain
  historical artifacts only through snapshot/export compatibility and audit
  rules.

The critical migration rule:

```text
For a given domain, either the old manifest path is authoritative or the new
control store is authoritative. Do not let both independently accept writes.
```

That rule is the operational form of the Single-Authority Invariant.

## Failure States

The design must define the failure table before implementation.

| Failure | Visibility | Recovery |
|---|---|---|
| transaction write fails | no new state | retry from request/idempotency key |
| transaction write succeeds, manifest write fails | no new state | orphan txlog cleanup or retry with same idempotency key |
| manifest write succeeds, pointer CAS fails | old state visible | orphan manifest cleanup; caller retries on new head |
| orphan transaction carries candidate metadata | not visible | treat as physical artifact gap; do not project unless revalidated |
| writer loses lease before CAS | no acknowledgement | new writer fences old epoch and recovers |
| writer acknowledges then crashes | committed token is visible | new writer reloads manifest and resumes |
| compactor is down | control writes continue | projection watermarks lag; system tables expose stale metadata |
| control compactor writes layout candidate but cannot publish | old layout visible | active writer publishes an equivalent layout update or GC cleans candidate |
| projection watermark update fails | projection files may exist but are not current | retry watermark publish through chosen root |
| projection compactor writes partial files | no new projection visible | retry projection publication |
| state token expires | no read-after-write retention guarantee | use later covering manifest or return `TokenExpired` |
| checkpoint expires | no long-read guarantee | reader renews or fails with `CheckpointExpired` |
| segment corruption detected | fail closed for control reads | repair from txlog/checkpoint/archive |
| provider CAS primitive is unavailable | no safe write path | disable backend for that provider |

## Prototype Performance Budgets

The ADR should set final numbers after measurement. The prototype should start
with explicit budgets so the team can tell whether the direction works.

Initial budgets:

| Metric | Prototype budget |
|---|---:|
| Warm write p99 for narrow metadata mutation | <= 250 ms |
| Warm point-read p99 | <= 50 ms |
| Warm prefix-scan p99 for bounded admin list | <= 150 ms |
| Cold writer startup to first write-ready state | <= 2 s |
| Maximum manifest-reachable replay on cold start | <= 64 MiB |
| Projection watermark lag for low-volume Tier-1 domains | <= 60 s target, explicit stale metadata beyond that |
| Control compaction backlog | alert before replay budget is exceeded |
| StateToken read-after-write retention | >= 1 hour for prototype |

These numbers are not product promises. They are guardrails for deciding whether
the design deserves ADR promotion.

## Testing And Verification

The prototype should not be accepted because happy-path DDL works. It needs
storage-engine-style tests.

Required tests:

- transaction commit publishes exactly one visible state token;
- user mutations advance `logical_sequence`;
- manifest publication assigns visible logical sequences to physical
  transaction IDs;
- orphan physical transaction files never become visible sequences unless
  revalidated and republished by a later valid manifest;
- compaction may advance `layout_generation` without advancing
  `logical_sequence`;
- pointer CAS loss leaves old state visible;
- stale writer epoch cannot publish;
- idempotency retry returns the original result;
- read-after-write by state token sees the mutation;
- expired state tokens return `TokenExpired` or use a later covering manifest;
- reads without the token obey documented freshness rules;
- range-empty and range-unchanged preconditions catch phantom writes;
- predicate preconditions catch stale authorization or path-governance inputs;
- replay from events equals folded KV state;
- projected Parquet rows equal the authoritative state through watermark N;
- checkpoints pin manifests and segments against GC;
- expired checkpoints release GC candidates;
- compaction preserves point reads and prefix scans;
- compaction layout publication cannot race user mutation publication;
- projection jobs ignore raw orphaned txlog objects;
- crash between transaction write and manifest publish is recoverable;
- crash after manifest publish but before in-memory memtable update is
  recoverable;
- corrupt transaction, manifest, and segment files fail closed;
- object-store listing is not used for request-time correctness;
- enforcement routes deny closed on stale or missing compiled state;
- authorization and credential vending never read Parquet system-table
  projections;
- revocation freshness and cache invalidation rules fail closed.

Operational tests:

- single writer takeover after lease loss;
- cold writer startup replay budget;
- warm writer latency budget;
- compactor outage with continued DDL success;
- projection lag surfaced in API and system-table metadata;
- provider matrix tests for conditional create, conditional replace, addressed
  read-after-write, stable version tokens, and checksums;
- provider adapter tests for timeout and retry semantics;
- prototype performance-budget tests for warm write, warm read, cold writer
  startup, replay bytes, and projection lag;
- downgrade and migration compatibility for file formats.

## Open Questions

1. Does Arco want metastore-scoped control-store roots before this migration, or
   should the MVP keep the current workspace mapping?
2. What retention period should control txlog files keep after compaction?
3. What retention period should the domain event archive use for each domain?
4. Should named checkpoints be user-visible, internal-only, or both?
5. Which routes must support token-pinned reads in the first release?
6. What compatibility guarantee does Arco want for control-store segment
    formats?
7. What revocation freshness bound should credential vending enforce?
8. Which compatibility APIs can expose state tokens through response bodies,
   headers, extension fields, or internal-only handling?
9. Which provider-specific CAS semantics are sufficient for production support?

## Decision Criteria

Move forward only if a prototype proves:

- DDL or governance mutation latency improves without weakening correctness;
- the compactor can be unavailable without blocking committed Tier-1 writes;
- read-after-write tokens preserve visible-success semantics;
- authorization and credential vending fail closed on stale state;
- replay equivalence holds across event log, folded KV state, and projections;
- operational complexity stays lower than running an external database;
- the storage format and dependency story are acceptable for Arco's release
  promises.

If the prototype cannot meet those criteria, keep ADR-018 as the authoritative
Tier-1 model and invest in derived point-lookup indexes instead.

Final architecture acceptance criteria:

1. A migrated Tier-1 domain has exactly one production write authority:
   control-store commit.
2. Successful Tier-1 mutations return `StateToken`s and do not wait for Parquet
   projection publication.
3. System tables expose projection watermarks and never claim stronger
   freshness than their watermark.
4. Authorization and credential vending never depend on lagging system tables.
5. Event replay equals folded KV state at each accepted token.
6. Parquet projections equal authoritative state through each projection
   watermark.
7. Workspace snapshots pin control-store checkpoints/state tokens plus
   projection watermarks.
8. Export manifests include all reachable authority, projection, event, and
   checkpoint artifacts.
9. Old-authority writes are disabled per domain at cutover.
10. `arco-state-current` has an explicit retirement plan.

## ADR Readiness

Approved for:

- `ArcoStateReader`, `ArcoStateStore`, `ArcoStateTxn`, and `ArcoStateAdmin`
  trait design;
- current-backend adapter;
- deterministic reference backend;
- Arco-native minimal object-store control-store prototype;
- shadow backend;
- provider CAS test harness;
- shadow replay and projection equivalence tests.

Not approved yet for:

- cutting over catalog DDL authority;
- moving credential vending or authorization to the new backend;
- introducing SlateDB as a dependency;
- declaring Parquet publication asynchronous for all Tier-1 domains;
- granting independent control-root CAS authority to multiple actors;
- committing to Arco-native segment formats.

## Bottom Line

Arco should separate Tier-1 mutation authority from Parquet projection
publication.

The safest path is an Arco-owned state-store contract, with the current
implementation behind it and a bounded object-store control-store prototype
behind the same interface. SlateDB should remain prior art for the storage
pattern, not a dependency in the recommended prototype path.

If the prototype succeeds, Arco gets a cleaner control-plane architecture:

```text
single-authority control-store commits
  -> returned state tokens
  -> async Parquet projection publication
  -> workspace snapshots and exports over retained cuts
```

That final state does not keep the old synchronous-compactor authority path as a
permanent peer. `arco-state-current` exists to migrate, shadow, validate
rollback, and read retained history.

That keeps Arco's strongest traits: deterministic replay, stable object IDs,
fenced publication, object-store-native deployment, and open queryable
projections.

## References

Local Arco references:

- `docs/adr/adr-018-tier1-write-path.md`
- `docs/adr/adr-032-immutable-manifest-pointers.md`
- `docs/adr/adr-034-fenced-head-published-control-plane-transactions.md`
- `docs/adr/adr-037-arco-catalog-product-surface.md`
- `docs/adr/adr-039-catalog-consistency-model.md`
- `docs/adr/adr-041-tiered-object-storage-orchestration-event-log.md`
- `docs/guide/src/reference/control-plane-scope.md`
- `docs/guide/src/reference/metastore-scope-architecture.md`
- `docs/plans/2026-06-26-arco-tier1-single-authority-combined-vision.md`
- `docs/plans/2026-06-20-olympia-inspired-arco-strategy.md`
- `crates/arco-catalog/src/writer.rs`
- `crates/arco-catalog/src/tier1_compactor.rs`
- `crates/arco-core/src/storage_traits.rs`
- `Cargo.toml`

External references checked on 2026-06-25:

- SlateDB design overview: <https://slatedb.io/docs/design/overview/>
- SlateDB object-store file layout: <https://slatedb.io/docs/design/files/>
- SlateDB checkpoints: <https://slatedb.io/docs/design/checkpoints/>
- SlateDB Rust crate page: <https://docs.rs/crate/slatedb/latest>
- SlateDB Rust `Db` API: <https://docs.rs/slatedb/latest/slatedb/struct.Db.html>
- SlateDB Rust `DbTransaction` API:
  <https://docs.rs/slatedb/latest/slatedb/struct.DbTransaction.html>
