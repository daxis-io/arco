> Status: Draft Phase 0 contract scaffold.
> Implementation status: Describes current baseline and proposed target semantics.
> Architecture status: The control-store path is prototype-approved only, not accepted production architecture.
> Compatibility status: Public/API exposure decisions are unresolved unless explicitly stated in this document.

# State Token And Checkpoint Contract

This draft separates current-baseline limitations from future token semantics.

## Current Baseline Limitations

The current implemented Tier-1 path is ledger append -> synchronous compaction
-> immutable manifest snapshot -> pointer CAS. It does not mean every current
Tier-1 write returns a future `StateToken`.

Current root transactions and manifest pointers can pin selected reads, but they
must not be described as equivalent to the future retained control-store
`StateToken` semantics unless a later ADR and implementation prove that mapping.

Current adapters should not mint fake tokens. If a future seam asks the current
adapter for unsupported token or checkpoint behavior, it should return an
explicit unsupported condition rather than a misleading token.

## Proposed StateToken Semantics

A future `StateToken` names retained authority state for a scope after a
successful migrated write.

Draft fields:

- `scope`;
- `logical_sequence`;
- `authority_manifest_id`;
- `issued_at`;
- `min_retained_until`;
- `capabilities`;
- `issuer`.

The token provides read-after-write against retained authority state for its
scope. It is not a general public compatibility guarantee until a surface
explicitly opts in.

## Proposed CheckpointToken Semantics

A future `CheckpointToken` is a stronger retention pin for:

- projection jobs;
- long scans;
- exports;
- backup;
- migration;
- replay-equivalence tests;
- point-in-time diagnostics.

Checkpoint tokens may retain more state than `StateToken`s and may have longer
or separately configured retention.

## Error Vocabulary

Draft token and checkpoint errors:

- `TokenExpired`: the token was valid but is outside its freshness or retention
  window.
- `CheckpointExpired`: the checkpoint retention pin is no longer available.
- `TokenScopeMismatch`: the token names a different authority scope than the
  request requires.
- `TokenUnsupported`: the current backend or API surface does not support the
  requested token behavior.
- `TokenNotRetained`: the token names state that is no longer retained or was
  never retained by the serving backend.
- `ProjectionTooStale`: the requested projection does not include the required
  authority sequence.

## Exposure Rules

Public/API token exposure remains unresolved unless a surface explicitly states
a current decision in `api-token-exposure-matrix.md`.

Compatibility APIs must not expose `StateToken`s in response bodies by default.
Headers, metadata, optional extension bodies, or internal-only bindings require
compatibility tests and a later decision.

## Open Decisions

- token signing or opaque-reference format;
- default token retention window;
- checkpoint retention policy;
- scope hierarchy;
- public exposure by API surface;
- cache and proxy behavior for header tokens;
- audit logging for token issuance and failed token use.
