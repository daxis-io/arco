> Status: Draft Phase 2 contract scaffold.
> Implementation status: Fixture-level conformance only.
> Architecture status: The control-store path is prototype-approved only, not accepted production architecture.
> Compatibility status: Archive retention is not yet a public export format guarantee.

# Domain Event Archive Retention

The domain event archive is the immutable event history retained for audit,
replay, migration, lineage, and export reconstruction. It is separate from the
control txlog used by the mutable control-plane implementation.

## Separation From Control Txlog

The control txlog contains transaction material needed to rebuild retained
authority state for the active implementation. It can have retention windows,
compaction, checkpointing, and garbage collection policies specific to that
implementation.

The domain event archive is retained under its own contract. Archive objects
needed for audit, export, replay equivalence, compatibility, or migration must
not be deleted merely because the mutable txlog has been compacted or garbage
collected.

## Replay And Export Requirements

- Replay-equivalence tests must use the archive contract, not the control txlog
  retention contract.
- Export manifests must include archive boundaries: cuts, ranges, watermarks,
  manifest references, or equivalent metadata.
- Snapshot and export services may pin archive ranges, but those pins must not
  create mutation-visible state.
- Archive retention decisions must be explicit and independently reviewable.
- Garbage collection must protect audit-required and export-required archive
  objects until every relevant retention pin has expired.
- Projection rebuilds that claim replay equivalence must declare which archive
  boundary they consumed.

## Archive Boundary Vocabulary

- `archive cut`: a single retained point in the domain event stream.
- `archive range`: an inclusive or half-open range of retained domain events.
- `archive manifest`: a manifest naming archive objects, checksums, schema
  versions, and relocation metadata.
- `export archive boundary`: the archive cut, range, or manifest reference
  included in an export manifest.

The exact serialized export format remains a follow-up decision.

## GC Protection Rules

Archive GC is policy-driven and independent from the mutable control-plane
txlog:

1. A compacted control txlog entry does not make the corresponding domain event
   archive object GC-eligible.
2. Audit policy, export policy, replay-equivalence policy, migration policy,
   and lineage retention policy can each keep archive objects reachable.
3. `WorkspaceSnapshot` and `ExportManifest` records pin the archive cut, range,
   or manifest needed to reconstruct the retained cut.
4. Projection artifacts are rebuildable only while their required archive
   boundary remains retained, or while a separate authoritative replay source is
   explicitly named.
5. Archive deletion requires evidence that no retained snapshot, export,
   checkpoint, root token, audit hold, migration hold, or replay-equivalence
   test requires the object.

## Follow-Up Tests

Phase 2 adds fixture-level contract coverage only. Code-level archive retention
tests should be added when the concrete archive manifest format lands:

- replay-equivalence fixture consumes domain archive boundaries rather than
  mutable txlog retention windows;
- export manifest fixture rejects missing archive cut/range/manifest metadata;
- GC fixture refuses to delete archive objects pinned by audit or export policy;
- projection rebuild fixture records the consumed archive boundary.
