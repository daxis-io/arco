# Tenant principal follow-up gates

The test-only principal event API proves identity-root mutation and replay. It
does not route authorization, revoke a metastore privilege, or purge a principal.
The production identity-root constructor remains closed.

## Gate 1: revocation across metastores

1. Define a named authorization cut containing both the identity token and the
   metastore token. Bind compiled permission cache entries to both tokens and
   the tenant and metastore root identities. A historical catalog token selects
   data only; it never selects an old identity lifecycle for enforcement.
2. At each authorization decision, validate the current identity lifecycle and
   membership revision against the named cut. Deny on missing, foreign,
   unreadable, or stale evidence. Grant writes must validate the principal at a
   named identity cut and retain that evidence in the metastore mutation.
3. Proposed qualification budget: a disable or membership revision must affect
   authorization in every metastore within **five seconds** of the identity
   authority commit. Enforcement must deny if its validated identity evidence
   is older than five seconds, even if a cache entry exists. This is a gate
   target, not behavior implemented by the principal API. Confirm the budget
   before admitting production routing.
4. Prove the bound with two or more metastores, cache hit and miss paths,
   concurrent grant and disable, stale/missing identity state, membership
   revisions, restart, and historical catalog reads pinned before disable.
   Retain exact identity and metastore tokens, decision times, and denial
   receipts. Qualify credential vending separately, including issued credential
   TTL and provider revocation behavior.

## Gate 2: disable, tombstone, retention-qualified purge

1. Specify a tombstone mutation and a distinct purge operation with durable
   transition receipts. Disable remains reversible only if a future policy
   explicitly admits re-enable; no such mutation is part of this slice.
2. Before purge, inventory every retained identity token and checkpoint,
   protected reference, audit/history record, and legacy-to-tenant migration
   mapping that can mention the principal. Preserve interpretation of those
   records after purge. Keep a permanent ID reservation so the ID cannot be
   assigned again.
3. Prove owned securables have been transferred or remain recoverable by an
   explicitly designated tenant administrator. Reject purge if ownership
   evidence is absent, stale, or ambiguous.
4. **Policy decision required:** set the minimum time between tombstone and
   purge, and specify which retained-token, audit, and migration-mapping
   horizons may outlive it. No principal purge rule is inferred from the
   physical root's 30-day history floor or seven-day object-age GC fence.
5. Exercise retained historical tokens before and after eligibility,
   interrupted purge and restart, conflicting ownership, migration mapping
   lookup, and an attempted ID reuse. Retain exact authority tokens and
   rejection receipts. Physical root GC only collects unreachable bytes; it
   does not authorize or execute principal purge.
