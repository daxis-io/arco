# ADR-044: Tenant Level Identity Authority

## Status

Accepted for authority ownership and the target contracts below. This decision
is not a claim that identity storage, cross-root authorization, or a migration
has been implemented or qualified.

## Context

The workspace-as-metastore pilot in [ADR-043](adr-043-s3-state-token-authority.md)
is compatibility behavior. Principals must remain stable across the workspaces
and metastores of a tenant without duplicating their identity or sharing their
privileges implicitly.

## Decision

A tenant owns one identity authority. Its principals can have independent
privileges in each metastore. Request provenance and durable authority identity
are separate concepts: a workspace can originate a metastore request without
becoming part of that metastore's durable key.

The ownership boundaries are:

| Authority | Prefix | Authoritative state |
|---|---|---|
| Tenant identity | `tenant={tenant}/identity/` | Users, service principals, workloads, groups, membership revisions, external identity bindings, principal lifecycle |
| Metastore | `tenant={tenant}/metastore={metastore}/` | Catalogs and securable objects, grants, ownership, compiled permissions, storage governance, workspace bindings |
| Workspace | `tenant={tenant}/workspace={workspace}/` | Execution and orchestration state |

Managed Delta tables have separate authority roots under ADR-043. Their exact
path and typed representation are a separate change; `AuthorityRoot` is
non-exhaustive so the initial families do not freeze the public enum.

A future identity store must accept a separate `IdentityMutation` family and
identity event envelope. Tenant provisioning, SCIM, and administrative events
need not supply a workspace or metastore. Their optional originating context
is provenance. Grants and storage-governance mutations must never be accepted
as identity mutations. `MetastoreLedger` remains restricted to workspace
compatibility and metastore roots.

## Required Cross-Root Contracts

These are requirements for the identity implementation and its authorization
consumers, not behavior supplied by the root type alone.

1. **Named authority cuts.** Compiled permissions and their cache identity record
   both the identity-state token and metastore-state token used to compile them.
   Consumers validate both roots and their required freshness before enforcement.
   A historical token pins data; it does not grant authorization. Missing or stale
   identity, permission, or binding evidence denies access until refreshed.
2. **Tenant-wide disable.** A disabled tenant principal is denied in every
   metastore, including requests pinned to historical catalog state. Historical
   grants may remain for audit and recovery; they cannot override current
   principal lifecycle state. The implementation must specify and qualify how
   current disable and membership revisions invalidate compiled permissions.
3. **Grant creation.** A grant validates its principal against a named identity
   cut and records that evidence with the metastore mutation. This does not claim
   cross-root atomicity. A concurrent disable still denies subsequent enforcement
   through the current identity check; workflows use explicit receipts and
   reconciliation for partially completed cross-root work.
4. **Lifecycle and retention.** Principal removal follows disable, then tombstone,
   then retention-qualified purge. Purge requires proof that retained tokens,
   audit/history references, and migration mappings remain interpretable. Stable
   principal IDs are not recycled. Physical deletion is not an immediate API
   effect of disabling a principal.
5. **Ownership recovery.** Before purge, owned objects must be transferred or
   remain recoverable by an explicitly designated tenant administrator. The
   ownership-recovery and retention checks are part of purge qualification.
6. **Credential vending.** Vending checks principal authorization, workspace
   binding, and metastore/storage authority using named identity and metastore
   cuts, then revalidates their freshness and lifecycle before minting. Changes
   require retry or denial. Already issued provider credentials follow their
   qualified revocation and TTL contracts; a cross-root check alone does not
   retroactively revoke them.
7. **Bootstrap.** Metastore creation names an existing tenant principal as its
   bootstrap administrator, validates that principal at a named identity cut,
   and records an idempotent bootstrap receipt. The first workspace caller does
   not implicitly become the owner.
8. **Migration and deduplication.** A migration inventories legacy principal and
   membership rows and persists a mapping from tenant, legacy root kind/ID, and
   legacy principal ID to a tenant principal ID. Deduplication uses verified
   external issuer/subject bindings or explicit administrative adjudication,
   never display names alone. Ambiguous or conflicting identities remain
   disabled or quarantined until resolved. Grants, ownership, and membership
   references are remapped and validated before routing switches. Retained
   history preserves the old-to-new mapping. Each migration needs a dry-run
   report, reconciliation evidence, and an explicit rollback boundary; no
   migration or automatic principal merge runs in this groundwork change.

## Implementation Boundary

This slice adds typed root values, root-specific identifiers, and prefix
construction. `matches_durable_root` checks ownership dimensions only; it checks
neither principal privileges nor workspace bindings. Domain services authorize
before acquiring mutation capabilities.

`ScopedStorage` retains the real request workspace for its existing workspace
and metastore constructors. It has no tenant-identity constructor. Identity
cannot enter legacy ledger, catalog, or state-store APIs by substituting the
tenant ID for a workspace. The current `StateScope` remains workspace-shaped;
`ControlMvpStateStore` rejects non-workspace physical roots, even when their IDs
have the same text. There is no implicit conversion from `AuthorityScope` to
that persisted representation.

The dedicated follow-up,
[Versioned AuthorityScope in StateScope and control/v1](../plans/2026-09-06-authority-root-review-revision.md#follow-up-versioned-authorityscope-in-statescope-and-controlv1),
must carry root kind through tokens, transactions, checkpoints, manifests,
projection intents, continuations, retained references, restore/GC checks, and
catalog bindings. It must explicitly version the serialized shape, preserve or
reject old workspace records according to a documented compatibility policy,
and prove that equal textual IDs in different root families cannot share
state identity. Identity CRUD and tenant-wide enforcement remain unavailable
until their separate semantic API and these contracts are implemented.

## Consequences

- Principal identity is shared within a tenant; privileges remain independent
  per metastore.
- Identity and metastore cuts can advance independently. No operation claims
  atomicity across them, and freshness is an enforcement requirement.
- The first workspace-as-metastore pilot retains its existing path bytes and
  persisted scope encoding. This decision does not relocate or promote a root.
- Existing metastore principal snapshots are compatibility data or derived
  authorization inputs during migration, not the target identity authority.

## Related References

- [Metastore scope architecture](../guide/src/reference/metastore-scope-architecture.md)
- [ADR-043: S3 StateToken Authority](adr-043-s3-state-token-authority.md)
