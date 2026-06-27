> Status: Draft Phase 0 contract scaffold.
> Implementation status: Describes current baseline and proposed target semantics.
> Architecture status: The control-store path is prototype-approved only, not accepted production architecture.
> Compatibility status: Public/API exposure decisions are unresolved unless explicitly stated in this document.

# Arco Storage Format V0

This file is not a stable public storage-format guarantee yet. It is a draft contract surface used to align current baseline behavior, target semantics, open decisions, and future conformance tests.

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

## Retention Surfaces

- `control txlog`: immutable control-store transaction records and any segment
  references needed to rebuild retained authority state.
- `domain event archive`: retained domain events needed for audit, replay,
  lineage, migration, and compatibility.
- `projection artifacts`: Parquet, index, or view artifacts selected by
  projection roots and annotated with projection watermarks.
- `snapshot/export pins`: retention records that keep authority state, event
  archive spans, projection artifacts, and package manifests reachable.
- `orphan grace period`: a configured delay before deleting unreferenced
  candidate artifacts after failed publication, CAS conflicts, or abandoned
  attempts.

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
