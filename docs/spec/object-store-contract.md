> Status: Draft Phase 0 contract scaffold.
> Implementation status: Describes current baseline and proposed target semantics.
> Architecture status: The control-store path is prototype-approved only, not accepted production architecture.
> Compatibility status: Public/API exposure decisions are unresolved unless explicitly stated in this document.

# Object Store Contract

This draft names the object-store behavior required by the current baseline and
the proposed control-store target. It is not an accepted provider certification
matrix.

## Required Primitives

The current path and proposed target both rely on object storage for durable
authority. Candidate provider primitives are:

- write immutable objects by explicit key;
- read immutable objects by explicit key;
- read byte ranges where an artifact format requires range access;
- publish a pointer with compare-and-swap or an equivalent fenced conditional
  update;
- reject stale pointer updates;
- preserve generation, etag, or version evidence needed for fencing and audit;
- expose enough object metadata for verification, repair, and GC;
- delete or expire unreferenced objects only after a retention policy allows it.

Object-store listing may support discovery, audit, repair, migration,
anti-entropy, and garbage collection. Listing is not required for request-time
correctness.

## Current Implemented Baseline

The current Tier-1 path writes immutable event, snapshot, and manifest artifacts,
then publishes visibility through a fenced pointer CAS. The compactor is the
writer of current public state snapshots and manifests for the current path.

## Proposed Target After Validated Cutover

The proposed target keeps object storage as durable authority, but changes the
success boundary for migrated scopes after validation and ADR acceptance:

```text
control transaction record
  -> control manifest
  -> control root pointer CAS
  -> StateToken
  -> derived projection publication
```

This target is prototype-approved only. It does not make the control-store path
accepted production architecture.

## Root Ownership And CAS Authority

Root ownership is the central split-brain control:

- The active authority writer owns mutation-visible control root updates.
- The projection compactor owns projection roots and public Parquet projection
  artifacts.
- No two roles have independent CAS authority over the same mutation-visible
  root.
- Snapshot and export services create retained cuts and packages, not mutation
  visibility.

Current migration and shadowing work must preserve the same rule. A shadow
control-store writer may compare, replay, and produce diagnostic artifacts, but
it must not publish a mutation-visible root for a scope still owned by the
current authority path.

## Provider Conformance Questions

Open provider questions include:

- which providers expose sufficient conditional-update evidence;
- whether pointer CAS can be implemented without relying on object listing;
- how generation, etag, version ID, or fence tokens are normalized;
- how failed candidate artifacts are retained long enough for repair;
- how cross-region or eventually consistent metadata behavior is tested;
- how provider-specific errors map to Arco authority errors.

These questions require follow-up conformance tests. This scaffold only records
the draft contract surface.
