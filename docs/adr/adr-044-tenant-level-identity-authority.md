# ADR-044: Tenant Level Identity Authority

## Status

Proposed

## Context

The current workspace-scoped behavior should be treated as compatibility
behavior, not as the principal model Arco ultimately wants to build around. A
tenant-level identity authority is the right direction, and introducing it is
part of defining the Lakehouse Catalog Metastore architecture.

## Decision

Introduce a tenant-level identity authority. Principals are globally stable
within a tenant rather than recreated inside each workspace or metastore.
Grants and ownership still belong to the metastore because they apply to
objects owned by that metastore, so a principal exists once at the tenant level
and can have completely different privileges in each metastore.

The durable boundaries are:

- `tenant={tenant}/identity/`
- `tenant={tenant}/metastore={metastore}/`
- `tenant={tenant}/workspace={workspace}/`

Each scope owns a distinct set of state:

- The **identity scope** owns users, service principals, workloads, groups,
  group memberships, external identity bindings, and principal lifecycle.
- The **metastore scope** owns grants, ownership, compiled permissions, storage
  governance, and workspace bindings.
- The **workspace scope** remains focused on execution and orchestration state.

## How It Works

The identity scope is the source of who a principal is, and it is shared across
the tenant rather than recreated per workspace or metastore. The metastore
scope is where the privileges for that principal are decided: because grants
and ownership apply to objects owned by a metastore, each metastore assigns its
own independent privileges to the same principal. A workspace is an access
context: it can reach a metastore only through an explicit binding, and a
principal can operate from multiple bound workspaces without gaining a separate
workspace-owned identity.

During the migration the workspace-as-metastore alias can remain in use, but it
is an implementation compatibility layer, not a permanent contract.

## Consequences

- The same principal can operate from multiple bound workspaces without
  creating a separate workspace-owned identity each time, and each metastore
  can assign completely independent privileges to that principal.


## Related References

- `docs/guide/src/reference/metastore-scope-architecture.md`
