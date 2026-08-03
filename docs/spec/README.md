# Draft Contract Specs

This directory holds draft contract scaffolds for the unified Arco execution
roadmap. Phase 2 extends these scaffolds with reader contracts, failure-mode
tables, root-ownership/IAM fixtures, provider-CAS fixtures, retention
reachability rules, and projection-watermark acceptance rows.

These files are not accepted ADRs, not stable public compatibility guarantees,
and not proof that the control-store path has cut over to production authority.
They align current baseline behavior, proposed target semantics, unresolved
decisions, and future conformance-test surfaces.

## Index

- `arco-storage-format-v0.md` - draft storage-format vocabulary, publication
  protocol surface, reader contracts, and GC reachability rules.
- `object-store-contract.md` - draft object-store primitive, root ownership,
  provider semantics, and failure-mode contract.
- `provider-capability-matrix.md` - draft provider certification states,
  evidence requirements, and disabled/read-only handling for ambiguous CAS.
- `root-ownership-and-iam-contract.md` - draft role/root matrix for CAS
  authority, mutation visibility, public Parquet writes, and engine boundaries.
- `domain-event-archive-retention.md` - draft separation between mutable
  control txlog retention and immutable domain event archive retention,
  including archive GC protection follow-ups.
- `state-token-and-checkpoint-contract.md` - draft token, checkpoint, retention,
  and error vocabulary.
- `projection-watermark-contract.md` - draft projection freshness and
  non-authority rules.
- `api-token-exposure-matrix.md` - draft compatibility matrix for token and
  watermark exposure.

## Fixtures

- `fixtures/provider-capability-matrix-v1.json` - fixture consumed by
  `crates/arco-core/tests/provider_capability_matrix_contract.rs`.
- `fixtures/root-ownership-and-iam-v1.json` - fixture consumed by
  `crates/arco-core/tests/root_ownership_iam_contract.rs`.

## Status Rules

- The implemented baseline remains ledger append -> synchronous compaction ->
  immutable manifest snapshot -> pointer CAS.
- The control-store path is prototype-approved only, not accepted production
  architecture.
- Public/API exposure decisions are unresolved unless a spec explicitly states a
  current decision.
- Follow-up ADRs are still required before Phase 0 is complete.
- Fixture-level provider and IAM tests do not certify live production providers
  or cloud IAM policies.
