# ADR-039: Catalog Consistency Model

## Status

Proposed

## Context

The catalog product surface expands Arco's control-plane state beyond
catalog/schema/table CRUD. Implementations need one consistency model for
ledger ordering, replay, compaction, publication, read-after-write behavior,
and stale projection errors.

## Decision

Catalog product state follows Arco's existing immutable publication model.

1. Mutations append immutable events with stable IDs, request IDs, actor
   identity, idempotency keys where retries can duplicate work, and deterministic
   hashing inputs.
2. Replay from genesis over the authoritative event stream yields the same typed
   state as the latest published projection, modulo documented compaction
   equivalence.
3. Compaction writes immutable projection artifacts and a snapshot manifest.
   Projection schemas are additive and include schema versions and ledger
   watermarks.
4. Visibility changes at fenced pointer movement. Readers see either the old
   complete snapshot or the new complete snapshot, never a partial projection
   set.
5. Ordinary reads use published snapshots or compiled views. They do not scan
   raw ledgers or object-store listings for correctness.
6. Search, lineage, discovery, and system tables may lag. Responses expose
   freshness watermarks or explicit stale/missing projection errors.
7. Rename changes aliases only. Object ID, grants, lineage bindings, governance
   attachments, storage binding, and audit identity remain stable.
8. Delete is a lifecycle transition or tombstone before physical cleanup.
9. Conflicting stale writes fail with a stable machine-readable error such as
   `conflict` or `precondition_failed`.
10. Missing, corrupt, unsupported, or stale projections fail closed for
    enforcement routes. Observability routes may return a bounded-staleness
    error when documented.
11. Credential vending, managed-commit coordination, and authorization use
    catalog-bound state and compiled views. Object-store listing may support
    repair, migration, or anti-entropy tools, but it is not the request-time
    correctness source for managed control-plane operations.

## Read-After-Write

For synchronous mutation APIs, a successful visible response means the mutation
is reflected in the published snapshot or in a transaction-pinned read token
returned by the API. A persisted-but-not-visible result must not be reported as
ordinary success.

Compatibility adapters may expose external response shapes, but they must
preserve Arco's visible-success semantics.

## Stale Or Missing State

Routes must distinguish safe observability staleness from unsafe enforcement
staleness:

| Route class | Missing or stale compiled state |
|---|---|
| read/list of ordinary object metadata | return `stale_projection` or bounded-staleness metadata when documented |
| mutation | deny or fail with `stale_projection`, `conflict`, or `precondition_failed` |
| authorization and credential mint | deny closed with auditable reason |
| system table query | return freshness/watermark metadata or a safe `stale_projection` error |
| repair and anti-entropy tools | may read raw artifacts but must not be tenant-visible enforcement paths |

## Consequences

- Projection publication and replay-equivalence tests are required for every
  implemented catalog domain.
- Enforcement routes deny on stale or missing compiled permissions unless the
  route explicitly accepts bounded staleness.
- System tables are safe operational views, not a shortcut around the
  authoritative replay and publication model.
