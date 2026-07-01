> Status: Draft Phase 0/2 contract scaffold.
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
- return a stable version token from a successful pointer write that can be
  used by a later conditional replace;
- detect checksum mismatches, truncated objects, or provider-reported
  corruption before treating fetched bytes as authoritative;
- apply bounded retry and timeout policy to mutation-visible operations;
- expose enough object metadata for verification, repair, and GC;
- delete or expire unreferenced objects only after a retention policy allows it.

Object-store listing may support discovery, audit, repair, migration,
anti-entropy, and garbage collection. Listing is not required for request-time
correctness.

## Required Provider Semantics

Any provider used for production control-plane writes must prove these
semantics before it can be marked write-enabled:

- `conditional create`: creating an immutable object at an explicit key must be
  fenced by a does-not-exist precondition or an equivalent provider-native
  primitive.
- `conditional pointer replace`: publishing a root pointer must use
  compare-and-swap against a stable provider version token, generation, etag, or
  equivalent fence.
- `stable version token`: a successful pointer write must return a token that
  can be supplied to a later conditional replace and is stable enough to record
  in audit and repair evidence.
- `addressed read-after-write`: after a successful conditional create or pointer
  CAS, reads by the exact object key or pointer key must observe the committed
  value or fail closed. Stale reads are not acceptable after the provider has
  acknowledged the successful write.
- `checksum or corruption detection`: fetched bytes used as authority input
  must be protected by provider checksum, Arco checksum, or another explicit
  corruption-detection mechanism.
- `retry and timeout policy`: mutation-visible operations must have bounded
  retries, bounded timeouts, and explicit mapping from provider errors to Arco
  authority errors.
- `no listing dependency`: request-time read correctness, write correctness,
  CAS conflict handling, and read-after-write checks must not depend on object
  listing.

Listing-dependent discovery may be used for maintenance, repair, offline audit,
anti-entropy, migration inventory, and garbage collection. It is not a
correctness mechanism for a request-time mutation or read-after-write decision.

When a provider's CAS semantics are ambiguous, emulated only through listing, or
not covered by repeatable evidence, that provider must remain disabled for
production writes. It may be represented as read-only or not-certified, but it
must not publish mutation-visible roots.

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

Detailed root ownership, IAM role boundaries, and provider certification rules
are split into:

- `root-ownership-and-iam-contract.md`
- `provider-capability-matrix.md`

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

## Failure-Mode Contract

The storage contract treats failed publication as invisible until a fenced root
selects the new state. Conformance tests should cover these outcomes before a
provider is production write-enabled.

| Failure mode | Required behavior | Follow-up evidence |
|---|---|---|
| Conditional create loses because the object exists | Existing object remains authoritative; writer reports duplicate or conflict without overwriting | Provider adapter test for does-not-exist precondition |
| Pointer CAS loses to a newer head | Old or competing new head remains visible; candidate artifacts become orphan candidates | CAS race test with stable version token evidence |
| Stale writer epoch attempts publication | Publication is rejected before mutation visibility changes | Writer fencing or lease-loss test |
| Duplicate retry after uncertain response | Retry is idempotent or reports a deterministic conflict; it must not publish twice | Idempotency/retry conformance test |
| Candidate manifest or segment is orphaned | Candidate remains invisible and is retained through orphan grace for repair/audit | GC reachability and orphan grace test |
| Addressed read after acknowledged write is stale | Read path fails closed instead of accepting stale authority bytes | Provider read-after-write test |
| Object bytes are truncated or corrupt | Reader rejects the artifact before authority or projection use | Checksum/corruption test |
| Provider timeout during mutation-visible operation | Operation maps to an explicit retryable/unknown/failed authority error | Timeout and retry policy test |
| `StateToken` retained state expires | Reader returns `TokenExpired` or `TokenNotRetained`; it must not serve older projection state as authority | Token retention test |
| `CheckpointToken` expires during a long read or projection job | Reader or projection job fails safely and reacquires a retained cut | Checkpoint renewal/expiry test |
| Projection watermark is stale | Projection reader returns stale status or `ProjectionTooStale` and does not claim stronger freshness | Projection watermark conformance test |
| Object listing is incomplete or stale | Request-time read/write correctness is unaffected | Negative test proving request path uses exact keys and manifests |

Until a live provider has repeatable evidence for the production-write rows in
this table, that provider remains read-only, disabled, or not certified for
mutation-visible roots.
