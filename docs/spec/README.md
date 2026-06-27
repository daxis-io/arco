# Draft Contract Specs

This directory holds draft Phase 0 contract scaffolds for the unified Arco
execution roadmap.

These files are not accepted ADRs, not stable public compatibility guarantees,
and not proof that the control-store path has cut over to production authority.
They align current baseline behavior, proposed target semantics, unresolved
decisions, and future conformance-test surfaces.

## Index

- `arco-storage-format-v0.md` - draft storage-format vocabulary and publication
  protocol surface.
- `object-store-contract.md` - draft object-store primitive and root ownership
  contract.
- `state-token-and-checkpoint-contract.md` - draft token, checkpoint, retention,
  and error vocabulary.
- `projection-watermark-contract.md` - draft projection freshness and
  non-authority rules.
- `api-token-exposure-matrix.md` - draft compatibility matrix for token and
  watermark exposure.

## Status Rules

- The implemented baseline remains ledger append -> synchronous compaction ->
  immutable manifest snapshot -> pointer CAS.
- The control-store path is prototype-approved only, not accepted production
  architecture.
- Public/API exposure decisions are unresolved unless a spec explicitly states a
  current decision.
- Follow-up ADRs are still required before Phase 0 is complete.
