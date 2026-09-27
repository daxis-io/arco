# Tenant identity-root physical lifecycle child

## Scope

Extend the default-disabled `test-utils` identity probe on format 9. Keep
`ControlMvpStateStore::new` closed to identity roots and retain the synthetic mutation
interface. This child proves physical storage behavior; it does not define
principals, revocation, purge, cross-root authorization, or production routing.

## Implementation

1. Use the existing retention lock and durable mutation epoch under the typed
   identity storage prefix. Checkpoint and protected-reference publication hold
   the epoch through exact immutable-write reconciliation. An unresolved
   publication leaves the epoch in flight until an operator proves all remote
   requests terminal and records a reason for recovery.
2. Publish immutable identity reference records that bind the typed state
   scope, exact manifest or checkpoint digests, and a deadline. Read them in
   bounded pages. Reject malformed, altered, foreign, or expired records.
   Workspace snapshot/export pins are never interpreted as identity roots.
3. Reuse the control/v1 reachability and reclamation fence. Protect the current
   authority closure, the normal historical token horizon, checkpoint floors,
   and every active identity reference closure. Recheck HEAD and exact object
   versions before each delete. Clean expired reference records in a separate
   bounded page after the authority inventory, under the same fence and epoch.

## Exit proof

- Identity checkpoint and reference publication survive restart and lost
  responses; an unresolved response blocks later lifecycle mutations until
  controlled terminal recovery.
- A historical manifest and checkpoint remain readable after HEAD advances and
  after the normal 30-day floor while their identity reference is active.
  Expiry permits collection after the age floor, without affecting current
  state.
- A paused reference publication excludes GC; malformed reference evidence
  aborts before deletion. Lost delete responses cannot restore an old
  authority generation or block safe retries.
- Workspace GC cannot list or delete identity objects. The production
  constructor continues to reject identity roots before I/O.

Passing this local memory-backend child is physical lifecycle evidence for the
test-only probe. Identity layout maintenance remains capped at 15 commits;
principal authority and production admission remain separate children.
