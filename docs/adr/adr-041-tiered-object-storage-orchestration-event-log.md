# ADR-041: Tiered Object-Storage Orchestration Event Log

## Status

Accepted

## Context

Arco orchestration is file-native. ADR-020 defines orchestration as a domain with
append-first facts, compacted Parquet projections, and manifest-published read
models. ADR-018 defines the authority boundary: API and control-plane components
append events and manage locks, while the compactor owns Parquet state files,
manifests, and published visibility.

The current orchestration ledger shape uses one object per event:

```text
ledger/orchestration/{event_ulid}.json
```

That layout is simple and replayable, but it is a poor hot path for high-volume
orchestration traffic. Heartbeats, task lifecycle facts, dispatch facts, timer
facts, and retry/cancellation events can create many small objects. This raises
object-store request cost, increases listing and anti-entropy work, and makes
fresh active-run scheduling harder to scale without a warm service.

A continuously running DuckDB or Quack checkpoint service could provide fast
fan-in, but it would introduce an always-on stateful service. That conflicts
with Arco's file-native and serverless posture. Quack is useful as a private
SQL transport, but it is not a durability boundary, a public worker API, or a
cross-tenant authority surface.

Arco needs a storage architecture that:

- keeps object storage as the durable authority;
- avoids one-object-per-event as the long-term hot path;
- preserves compactor ownership of canonical ledger segments, projections, and
  manifest publication;
- scales to zero when idle;
- supports active-run compaction without relying on reader-triggered refreshes;
- allows DuckDB and Quack to accelerate hot paths without becoming required
  production dependencies.

## Decision

Adopt a tiered object-storage event log for orchestration.

The production architecture is:

```text
L0 inbox bundles       many-writer ingestion fan-in
L1 ledger segments     canonical ordered replay log
L2 projections         compactor-owned SQL/read model
manifest pointers      visibility boundary
```

DuckDB and Quack are optional accelerators. They may run inside active workers,
active-run orchestration actors, compactors, or local development loops, but
they are not required for durability or authority.

### Storage Layout

The orchestration storage layout becomes:

```text
{bucket}/{tenant}/{workspace}/
├── _inbox/orchestration/
│   └── run={run_id}/
│       └── producer={producer_id}/
│           └── attempt={attempt_id}/
│               ├── bundle={bundle_ulid}.parquet
│               └── bundle={bundle_ulid}.jsonl.zst
├── receipts/orchestration/
│   └── run={run_id}/
│       └── producer={producer_id}/
│           └── receipt_segment={receipt_ulid}.json
├── ledger/orchestration/segments/
│   └── shard={shard_id}/
│       └── seq={start_seq}-{end_seq}.parquet
├── ledger/orchestration/index/
│   └── shard={shard_id}/
│       ├── current.json
│       └── history/{commit_ulid}.json
├── state/orchestration/
│   ├── runs/snapshot={snapshot_id}/part-*.parquet
│   ├── tasks/snapshot={snapshot_id}/part-*.parquet
│   ├── attempts/snapshot={snapshot_id}/part-*.parquet
│   ├── dispatch_outbox/snapshot={snapshot_id}/part-*.parquet
│   └── timers/snapshot={snapshot_id}/part-*.parquet
├── manifests/orchestration/
│   ├── current.json
│   └── history/{snapshot_id}.json
└── quarantine/orchestration/
    └── reason={reason}/event_or_bundle={id}.json
```

### Authority Boundaries

Workers never write canonical ledger segments, projection state, manifests,
commits, snapshots, or sequence heads.

The default worker path remains callback-based:

```text
Worker
  -> callback API with task token and attempt identity
  -> stateless ingest endpoint
  -> L0 inbox bundle
```

The ingest endpoint validates tenant, workspace, run, task, attempt, token,
callback path, and event type before writing L0. Untrusted workers do not receive
object-store write authority.

Trusted direct upload is allowed only as a controlled runtime optimization. In
that mode, the control plane may grant a worker narrow write permission to its
own L0 prefix:

```text
_inbox/orchestration/run={run_id}/producer={producer_id}/attempt={attempt_id}/
```

Trusted workers still cannot write:

```text
ledger/
state/
manifests/
sequence/
commits/
snapshots/
```

The compactor role is the only writer of:

- canonical L1 ledger segments;
- L1 shard indexes;
- L2 projection files;
- manifest pointers;
- projection snapshots;
- quarantine outputs for invalid canonicalization inputs.

### L0 Inbox Bundles

L0 is the cheap fan-in layer. It accepts many writers without global ordering.

L0 bundle events carry:

```text
event_id
tenant_id
workspace_id
run_id
task_key
attempt_id
event_type
producer_id
producer_seq
occurred_at
payload
schema_version
```

L0 bundle metadata carries:

```text
bundle_id
tenant_id
workspace_id
run_id
task_key
producer_id
attempt_id
event_count
min_producer_seq
max_producer_seq
schema_version
format
compression
checksum
created_at
```

The compactor dedupes by both:

```text
event_id
producer_id + producer_seq
```

Task identity is scoped as:

```text
tenant_id + workspace_id + run_id + task_key
```

Attempt identity is scoped as:

```text
tenant_id + workspace_id + run_id + task_key + attempt_id
```

The system must not dedupe or authorize facts by `task_key` alone.

An L0 write means the event was accepted for canonicalization. It does not mean
the event is part of the canonical orchestration ledger.

### L1 Canonical Ledger Segments

L1 is the durable canonical replay log.

The compactor promotes accepted L0 bundles into ordered, deduped L1 segments:

```text
ledger/orchestration/segments/shard={shard_id}/seq={start_seq}-{end_seq}.parquet
```

Workers and ingest endpoints do not assign global sequence positions. They only
provide stable event IDs and producer sequence numbers.

The compactor assigns:

```text
sequence_position
shard_id
segment_id
canonical_commit_id
```

Shard assignment is based on the scheduling and replay boundary:

```text
shard = hash(tenant_id, workspace_id, run_id) % shard_count
```

Each shard has its own sequence head:

```text
ledger/orchestration/index/shard={shard_id}/current.json
```

This avoids a single global sequence object as the bottleneck.

### L2 Projections and Manifests

L2 is the read model and evidence surface.

The compactor folds L1 segments into projection tables under:

```text
state/orchestration/
```

Readers discover visible state through:

```text
manifests/orchestration/current.json
```

Readers must not discover current state by listing projection directories.

`system.orchestration.*` remains read-only evidence over manifest-published
projection files. It is not the enforcement path for dispatch, retry,
cancellation, timer, or authorization decisions.

A manifest pointer is the visibility boundary. Files not referenced by a current
or retained historical manifest are not visible to readers and may be garbage
collected after retention rules allow it.

### Receipt Semantics

Receipts must not recreate one-object-per-event overhead.

The system supports receipt segments and targeted terminal receipts. Receipt
entries distinguish these states:

```text
accepted_l0
published_l1
visible_l2
quarantined
```

Definitions:

- `accepted_l0`: an ingest endpoint or trusted upload wrote the event into an L0
  bundle.
- `published_l1`: the compactor included the event in a canonical L1 segment.
- `visible_l2`: the compactor folded the event into a manifest-published L2
  projection.
- `quarantined`: validation rejected the event or bundle and wrote durable
  rejection evidence.

Worker cleanup policy:

- `TaskStarted` remains in the worker local outbox until `published_l1`.
- `TaskFinished` remains in the worker local outbox until `published_l1`; callers
  that need evidence for UI/read-model visibility may wait for `visible_l2`.
- `TaskHeartbeat` may be coalesced; the latest pending heartbeat is sufficient.
- Task metrics should be aggregated into terminal facts or written to a separate
  telemetry domain.
- Business or ELT transaction rows must not flow through the orchestration
  ledger. Orchestration facts reference output artifacts, row counts, checksums,
  schema fingerprints, and metrics.

### Compaction Modes

The system supports three compaction modes.

Event-triggered compaction is used for active runs. It wakes on L0 bundle
creation, dispatch-relevant events, timer events, terminal events, or explicit
run activity. Scheduling, retries, cancellation, and timers use this mode.

Synchronous compaction is used when a control-plane operation requires
read-after-write behavior. The caller obtains a lock or lease, sends explicit
inputs to the compactor, and waits for manifest publication.

Reader-triggered lazy compaction is allowed for dashboards, inspection,
exploratory reads, evidence refresh, and anti-entropy. It is not allowed to own
dispatch decisions, retries, cancellations, timers, or worker cleanup semantics.

Object notifications are hints, not correctness. Scheduled sweeps and
read-through repair must discover missed L0 bundles.

### Lease and Fencing Model

Compactors and active-run actors use scoped leases with fencing tokens.

A lease record includes:

```json
{
  "lease_id": "01J...",
  "tenant_id": "t1",
  "workspace_id": "w1",
  "shard_id": "000",
  "holder_id": "compactor-abc",
  "fencing_token": 42,
  "expires_at": "2026-06-06T15:01:00Z"
}
```

Every L1 segment commit, L2 projection commit, and manifest publish includes the
fencing token. A stale holder cannot publish after losing the lease.

Object-store conditional writes are required for:

- create-if-absent L0 bundle writes;
- create-if-absent L1 segment writes;
- create-if-absent projection file writes;
- compare-and-swap manifest pointer updates;
- compare-and-swap L1 shard index updates;
- lease acquisition and renewal.

### Active-Run DuckDB Actor

An active-run DuckDB actor is a future optimization, not the base architecture.

It may run while a tenant, workspace, run, or shard is active. It can use local
DuckDB for scheduling, validation, folding, joins, and dispatch queue decisions.
When idle, it exits. On restart, it rebuilds from L1 plus accepted L0 tail.

Before an active-run actor can own hot scheduling, the implementation must prove:

- run or shard lease acquisition with fencing;
- deterministic replay from L1 plus accepted L0 tail;
- duplicate-dispatch protection;
- idempotent callback and event ingestion;
- manifest-published dispatch outbox projection;
- actor crash recovery to the same state;
- stale actor rejection after lease loss.

The local DuckDB file is a cache and working set. It may be checkpointed and
uploaded as a debug or resume artifact, but it is not required for correctness.
Object storage remains the durable authority.

### Quack

Quack is an optional private accelerator.

Allowed uses:

- trusted internal fan-in to an active DuckDB actor;
- local development parity;
- hosted or controlled worker pools;
- callback replicas forwarding already validated batches.

Disallowed uses:

- public worker API;
- source of durability;
- manifest or state writer;
- cross-tenant SQL surface;
- required production dependency;
- replacement for callback token and active-attempt validation.

Quack endpoints must be private, feature-gated, and backed by custom
authorization. A fallback to the callback ingest path is required.

### Duplicate Dispatch Protection

Dispatch IDs are deterministic:

```text
dispatch_id =
  hash(tenant_id, workspace_id, run_id, task_key, attempt_id, dispatch_kind)
```

The lifecycle is represented as facts:

```text
DispatchRequested
DispatchEnqueued
TaskStarted
TaskFinished
```

Only one effective `DispatchRequested` and one effective `DispatchEnqueued` are
allowed per `dispatch_id`.

`TaskStarted` and `TaskFinished` must match the active attempt identity. Late
facts for superseded attempts fold to ignored or stale state, not current task
state.

Projection folding owns derived states such as:

```text
ready
queued
running
succeeded
failed
skipped
retry_wait
cancelled
```

Workers do not write derived states directly.

## Non-Goals

- Do not use a continuously running DuckDB or Quack server as orchestration
  authority.
- Do not expose Quack as the public worker API.
- Do not allow workers to write L1, L2, manifests, snapshots, sequence heads, or
  commits.
- Do not route business transaction rows through the orchestration ledger.
- Do not use reader-triggered lazy compaction for scheduling, retry,
  cancellation, timers, or worker cleanup semantics.
- Do not mark active-run DuckDB actors or trusted direct upload as shipped until
  their lease, replay, fencing, and recovery tests exist.
- Do not remove callback/token validation from the default worker path.

## Superseded Conventions

For orchestration hot-path writes, this ADR supersedes the one-object-per-event
layout as the long-term production target:

```text
ledger/orchestration/{event_ulid}.json
```

ADR-020 remains the source of truth for the orchestration domain model:
append-first facts, projection-only derived state, and manifest-published read
models.

ADR-018 remains the source of truth for the compactor authority boundary. This
ADR refines the orchestration-specific write path so API and ingest components
write `_inbox/`, while the compactor writes canonical L1 ledger segments, L2
state, and manifests.

## Consequences

### Positive

- Reduces small-object pressure from high-volume orchestration events.
- Preserves object storage as the durable authority.
- Keeps compactor ownership of canonical ledger segments, projections, and
  manifest visibility.
- Supports scale-to-zero when no runs or compaction work are active.
- Allows active-run acceleration without requiring a warm database.
- Keeps untrusted workers behind callback/token validation.
- Makes replay deterministic from L1 plus accepted L0 tail.
- Allows object-store IAM to enforce writer boundaries.

### Negative

- Adds an ingestion tier and canonicalization step.
- Makes receipt semantics more explicit.
- Requires compactor leases, fencing, and CAS tests for L1 and L2 publication.
- Requires anti-entropy sweeps because object notifications are not correctness.
- Adds migration work from one-event ledger objects to segmented ledger reads.

### Mitigations

- Use event-triggered compaction for active runs.
- Use synchronous compaction for read-after-write control-plane operations.
- Keep lazy compaction limited to evidence refresh and anti-entropy.
- Use receipt segments instead of per-event receipts.
- Gate trusted direct upload, active DuckDB actors, and Quack behind explicit
  feature flags.
- Preserve compatibility readers for existing one-event ledger objects during
  migration.

## Verification Requirements

Architecture tests must prove:

- workers cannot write L1, L2, manifests, snapshots, sequence heads, or commits;
- untrusted workers use callback ingest and token validation;
- L0 bundle writes are create-if-absent;
- duplicate L0 bundles dedupe by `event_id` and `producer_id + producer_seq`;
- task and attempt validation uses tenant, workspace, run, task key, and attempt
  identity;
- compactor-only L1 segment publication;
- compactor-only L2 projection and manifest publication;
- stale fencing token rejection;
- manifest CAS loss does not publish visible state;
- missed object notifications are recovered by sweep or read-through repair;
- worker cleanup waits for `published_l1` for terminal facts;
- reader-visible `system.orchestration.*` state changes only after manifest
  publication;
- active-run actor crash recovery replays to the same state before the actor
  feature can be enabled;
- Quack fan-in falls back to callback ingest and cannot bypass authorization.

## Implementation Phases

### Current Implementation Status

As of 2026-06-06, Arco implements the first additive contract slice for ADR-041:

- canonical path builders for `_inbox/orchestration`, receipt segments, L1
  segment files, and L1 shard indexes;
- an `arco_flow::orchestration::event_log` L0 inbox bundle schema and Parquet
  writer;
- create-if-absent bundle writes through the existing storage abstraction;
- validation for bundle scope, storage tenant/workspace binding, task-key
  consistency for the attempt, schema version, duplicate `event_id`, and
  duplicate `producer_id + producer_seq`.

This slice does not route the default worker callback path through `_inbox/` and
does not implement L0-to-L1 promotion, L1 shard-index CAS publication, L2
projection publication, active-run DuckDB actors, trusted direct upload, or
Quack fan-in.

### Phase 1: L0 Inbox and L1 Segments

- Add callback ingest support for L0 bundles.
- Define L0 bundle schema and metadata.
- Add receipt segment writer.
- Add stateless compactor promotion from L0 to L1.
- Add dedupe by `event_id` and `producer_id + producer_seq`.
- Add CAS and fencing tests for L1 shard index publication.

### Phase 2: L2 Projections and Manifest Read Model

- Fold L1 segments into `runs`, `tasks`, `attempts`, `dispatch_outbox`, and
  `timers` projections.
- Publish `manifests/orchestration/current.json`.
- Route `system.orchestration.*` through manifest-published projection files.
- Add freshness, stale-read, and watermark tests.

### Phase 3: Active-Run DuckDB Actor

- Add run or shard lease acquisition.
- Add deterministic replay from L1 plus accepted L0 tail.
- Add duplicate-dispatch protection.
- Add crash/restart and stale-holder rejection tests.
- Add final flush and shutdown protocol.

### Phase 4: Quack Trusted Fan-In

- Add private-only Quack endpoint for active actors.
- Add custom authorization hook and narrow insert-only surface.
- Add trusted-worker feature flag.
- Add fallback to callback ingest.

## Related ADRs

- ADR-018: Tier-1 Write Path Architecture
- ADR-020: Orchestration as Unified Domain
- ADR-021: Cloud Tasks Naming Convention
- ADR-022: Per-Edge Dependency Satisfaction
- ADR-023: Worker Contract Specification
- ADR-032: Engine Boundaries and Split-Service Topology
- ADR-032: Immutable Manifest Snapshots + Pointer CAS
- ADR-034: Fenced Head-Published Control-Plane Transactions
- ADR-035: System Catalog Tables
- ADR-040: Execution Locations

## References

- DuckDB Quack Remote Protocol: https://duckdb.org/docs/current/quack/overview
- DuckDB Quack Security: https://duckdb.org/docs/current/quack/security
- DuckDB CHECKPOINT Statement: https://duckdb.org/docs/current/sql/statements/checkpoint.html
- Amazon S3 Conditional Writes: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
- Amazon S3 Consistency Model: https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel
- Google Cloud Storage Request Preconditions: https://cloud.google.com/storage/docs/request-preconditions
- Cloud Run Autoscaling: https://cloud.google.com/run/docs/about-instance-autoscaling
