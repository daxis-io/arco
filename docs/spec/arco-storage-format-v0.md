> Status: Draft Phase 0/2 contract scaffold.
> Implementation status: Describes current baseline and proposed target semantics.
> Architecture status: The control-store path is prototype-approved only, not accepted production architecture.
> Compatibility status: Public/API exposure decisions are unresolved unless explicitly stated in this document.

# Arco Storage Format V0

This file is not a stable public storage-format guarantee yet. It is a draft
contract surface used to align current baseline behavior, target semantics,
open decisions, and future conformance tests.

## Current Implemented Baseline

The implemented Tier-1 authority path is:

```text
ledger append
  -> synchronous compaction
  -> immutable manifest snapshot
  -> pointer CAS
```

Catalog DDL currently uses the catalog writer, an explicit ledger append, a
synchronous compactor, immutable snapshot artifacts, immutable manifests, and a
fenced pointer CAS for visibility. Readers use pointer-selected snapshots and
manifests. Object-store listing may support repair, audit, migration, and
anti-entropy, but it is not required for request-time correctness.

The current baseline does not prove that catalog DDL has cut over to a new
control-store authority, that credential vending reads a new control store, or
that every current Tier-1 write returns a future `StateToken`.

## Proposed Target After Validated Cutover

After a validated prototype, an accepted ADR, and a per-domain cutover, a
migrated Tier-1 scope may use:

```text
domain service
  -> ArcoStateTxn
  -> object-store-backed control-store transaction
  -> fenced control manifest pointer CAS
  -> StateToken returned to caller
  -> asynchronous projection compactor
  -> watermarked Parquet projection artifacts
```

The current ledger plus synchronous compactor path may remain only as a
migration adapter, shadow replay input, rollback aid, or retained-history
compatibility path for migrated domains. It must not become a permanent peer
mutation authority for a migrated scope.

## Authority Roots And Projection Roots

Draft vocabulary:

- `control root`: the mutation-visible pointer for a control-store authority
  scope.
- `projection root`: the pointer selecting derived projection artifacts for a
  surface such as system tables, audit views, lineage, search, or derived
  indexes.
- `retained cut`: a snapshot/export/checkpoint retention boundary over authority
  state, event archives, and projection watermarks.

For any future migrated scope, the active authority writer owns the
mutation-visible control root. The projection compactor owns projection roots
and public Parquet projection artifacts. Snapshot and export services create
retained cuts and packages, not mutation visibility. No two roles may have
independent CAS authority over the same mutation-visible root.

The detailed role matrix and IAM boundaries live in
`root-ownership-and-iam-contract.md`. Provider certification requirements live
in `provider-capability-matrix.md`. This file names the storage-format
vocabulary and links to those contracts instead of duplicating them.

## Current Publication Protocol

The current publication protocol is:

1. Append an immutable ledger event or transaction record.
2. Pass explicit event paths to the synchronous compactor.
3. Write immutable snapshot and manifest artifacts.
4. CAS-publish the manifest pointer with the relevant fence.
5. Return success after the published snapshot is visible.

This protocol remains the implemented baseline for the current path.

## Proposed Control-Store Publication Protocol

The proposed protocol for a future accepted control-store scope is:

1. Route the write to the active authority writer for the scope.
2. Begin an `ArcoStateTxn` over retained authority state.
3. Validate preconditions, idempotency, object generations, and authorization
   inputs against authority state.
4. Write an immutable control transaction record.
5. Write folded state, segments, checkpoints, or manifest-selected artifacts.
6. Write the next immutable control manifest.
7. CAS-publish the control root pointer.
8. Return a `StateToken` naming the retained authority sequence.
9. Let projection workers publish derived artifacts with
   `ProjectionWatermark`s.

This is proposed target semantics only. It is not accepted production
architecture in this draft.

## Snapshot, Export, And Root Token Vocabulary

- `StateToken`: a read-after-write token naming a logical sequence and retained
  authority state for one scope.
- `CheckpointToken`: a stronger retention pin for projection jobs, long scans,
  export, migration, backup, and replay-equivalence checks.
- `ProjectionWatermark`: the highest authority logical sequence included in a
  derived projection.
- `WorkspaceSnapshot`: a retained cut over authority checkpoints or state
  tokens, projection watermarks, event archive boundaries, and retention
  metadata.
- `ExportManifest`: a portable package manifest listing required authority,
  event, checkpoint, projection, checksum, compatibility, and relocation
  objects.
- `RootToken`: a token or manifest reference that pins a multi-root read cut.
  Its exact scope and compatibility exposure remain open.

## Reader Contracts V0

External readers do not implement the hot writer protocol. The readable
contract is split by authority role.

### Authority Reader

An authority reader is internal to Arco services that need current or
token-pinned control state:

1. Read at the current authority root or at a supplied `StateToken`.
2. Resolve a retained control manifest, or a later manifest that covers the
   requested logical sequence.
3. Validate scope, logical sequence, manifest hash, layout version, checksum,
   and token retention.
4. Read only manifest-selected authority artifacts through the state-reader
   interface for that scope.
5. Fail closed when enforcement, credential vending, or policy reads require a
   fresher authority cut than the resolved state provides.
6. Never require object-store listing for request-time correctness.

### Projection Reader

A projection reader is the public/read-only contract for system tables, audit
views, lineage/search views, exports, and external inspection:

1. Resolve a projection root, `WorkspaceSnapshot`, or `ExportManifest`.
2. Read the selected projection manifest and declared
   `ProjectionWatermark`.
3. Validate projection schema version, source authority root or token, covered
   logical sequence, artifact hashes, and lag policy.
4. Read only manifest-referenced Parquet, JSON, or index artifacts.
5. Return explicit stale-projection status when the watermark is below the
   required sequence for the requested use.
6. Treat projections as derived read models, never as mutation or enforcement
   authority.

### Root-Token Reader

A root-token reader resolves a pinned multi-root cut:

1. Resolve the root transaction, root token record, or super-manifest by exact
   key.
2. Require the record to be visible, retained, and layout-compatible.
3. Resolve referenced authority heads and projection watermarks directly.
4. Read only artifacts reachable from the pinned record.
5. Fail with an explicit expired or unsupported-token condition when the cut is
   no longer retained.

### Snapshot And Export Reader

A snapshot/export reader reconstructs an immutable retained cut:

1. Resolve the `WorkspaceSnapshot` or `ExportManifest` by exact key.
2. Validate checksums, layout versions, retention deadline, archive boundaries,
   relocation metadata, and projection watermarks.
3. Pin or verify all authority, checkpoint, domain event archive, projection,
   and package artifacts required by the record.
4. Reject exports that omit required event archive boundaries for audited or
   replay-equivalent domains.
5. Never create mutation visibility while serving a snapshot or export.

## Retention Surfaces

- `control txlog`: immutable control-store transaction records, current-path
  ledger material, and any segment references needed to rebuild retained
  authority state inside the mutable control-plane implementation.
- `domain event archive`: immutable domain events retained under a separate
  replay, audit, lineage, migration, and export contract.
- `projection artifacts`: Parquet, index, or view artifacts selected by
  projection roots and annotated with projection watermarks.
- `snapshot/export pins`: retention records that keep authority state, event
  archive spans, projection artifacts, and package manifests reachable.
- `orphan grace period`: a configured delay before deleting unreferenced
  candidate artifacts after failed publication, CAS conflicts, or abandoned
  attempts.

Control txlog retention and domain event archive retention are separate
contracts. Garbage collection of txlog material must not imply garbage
collection of immutable archive objects required for audit, replay equivalence,
or export reconstruction. Export manifests must declare the archive boundary
they cover, such as a cut, range, watermark, or manifest reference.

The detailed archive retention contract lives in
`domain-event-archive-retention.md`.

## GC And Retention Reachability

Garbage collection starts from retained roots, not from object listing. Listing
may discover candidates, but a candidate is deletable only after reachability
and retention checks prove no retained cut requires it.

| Reachability root | Protects | Deletable only after |
|---|---|---|
| Current authority root | Current control manifests, segments, txlog records, and folded state selected by the visible head | A newer visible root no longer references them and token/checkpoint retention has expired |
| `StateToken` | Authority state covering its logical sequence and scope | `min_retained_until` passes and no later retained manifest is required to serve the token |
| `CheckpointToken` | Authority state, manifests, and segments needed by long reads, projection jobs, backup, migration, or replay checks | The checkpoint is released or expires and no snapshot/export/root token references it |
| `WorkspaceSnapshot` | Authority heads or checkpoints, projection watermarks, archive boundaries, and retention metadata for the cut | The snapshot retention policy expires and no export or root token references it |
| `ExportManifest` | Authority, checkpoint, archive, projection, checksum, compatibility, and relocation artifacts required for restore or audit | The export retention policy expires and audit/export policy allows deletion |
| Projection root | Projection manifests and derived Parquet/index artifacts selected by the current watermark | A newer projection root replaces them and no snapshot/export/root token pins the older watermark |
| Domain event archive boundary | Immutable domain events needed for audit, replay equivalence, lineage, migration, or export reconstruction | Every applicable audit, replay, migration, and export retention policy allows deletion |
| Orphan grace record | Candidate artifacts from failed CAS, duplicate retry, abandoned transaction, or partial publication | The grace period expires and the candidate is unreachable from every retained root |

GC must not delete domain event archive objects merely because the mutable
control txlog has compacted. Archive retention and txlog retention are separate
contracts.

## Phase 2 Contract Links

- `object-store-contract.md` defines the provider primitives and request-time
  correctness rules.
- `provider-capability-matrix.md` defines provider certification states and
  required evidence.
- `root-ownership-and-iam-contract.md` defines CAS authority and role/root
  boundaries.
- `domain-event-archive-retention.md` defines archive retention, replay, and
  export boundaries.

## Failure States And Open Decisions

Draft failure states:

- authority pointer CAS conflict;
- orphaned candidate manifest or segment;
- missing retained authority state;
- expired checkpoint;
- projection watermark below a required sequence;
- incompatible response-body token exposure for a compatibility API;
- split root ownership attempt;
- provider generation or fencing mismatch.

Open decisions:

- control root scope: metastore, workspace, domain-sharded, or another explicit
  scope;
- default `StateToken` and `CheckpointToken` retention windows;
- root token scope and public exposure;
- first writable control-store domain;
- snapshot/export package layout;
- orphan grace period and GC proof requirements;
- conformance fixture format and versioning.
