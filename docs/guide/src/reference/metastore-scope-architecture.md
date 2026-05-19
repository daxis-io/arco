# Metastore Scope Architecture

This page defines the target scope boundary for Arco's lakehouse catalog
metastore. It is an architecture target and migration guide, not a claim that
the current repository already stores all catalog state at this scope. For
repo-local implementation status, use the [control-plane scope scorecard](./control-plane-scope.md).

## Decision

Arco should model `metastore_id` as a first-class logical scope.

The metastore is the governed catalog authority: it owns catalogs, schemas,
tables, views, volumes, grants, storage governance, credential vending policy,
lineage bindings, governance attachments, and system projections for those
objects. A workspace is an execution and access context: it runs workloads,
queries, orchestration, notebooks, pipelines, and API requests against a bound
metastore.

The first implementation may map:

```text
metastore_id = workspace_id
```

That compatibility mapping keeps the MVP simple while preventing the public
contract from assuming that a workspace and a metastore are the same thing.

The target model is:

```text
tenant
  -> metastore
      -> governed catalog state
      -> stable object IDs
      -> grants and compiled permissions
      -> storage governance and credential scope
  -> workspace
      -> execution state
      -> request context
      -> orchestration state
      -> explicit binding to one or more metastores
```

## Terms

| Term | Meaning |
|---|---|
| Tenant | Top-level isolation boundary for an organization or deployment customer. |
| Metastore | Governed catalog authority within a tenant. It owns object identity, grants, storage governance, and catalog projections. |
| Workspace | Compute, API, and orchestration context. A workspace can access a metastore only through an explicit binding. |
| Workspace binding | Authoritative relationship that permits a workspace to use a metastore or a specific metastore-owned object. |
| Managed root | Storage root governed by the metastore, optionally constrained to a workspace binding or catalog/schema scope. |
| System table | Read-only, redacted projection over published catalog state. It is not an enforcement input. |

## Why This Boundary Matters

If the metastore is physically and logically scoped only by workspace, shared
governance becomes difficult. A tenant with `notebooks`, `pipelines`, and `bi`
workspaces would either duplicate catalog state or need out-of-band sync. That
breaks the core promise that grants, object IDs, storage bindings, and lineage
are authoritative.

If the metastore is a tenant-level object with explicit workspace bindings,
multiple workspaces can safely share one governed catalog:

```text
tenant=acme
  metastore=lakehouse_prod
    catalogs=sales, finance, ml
    grants=shared
    external_locations=shared
    stable table IDs=shared

  workspace=notebooks -> bound to lakehouse_prod
  workspace=pipelines -> bound to lakehouse_prod
  workspace=bi        -> bound to lakehouse_prod
```

This gives Arco the shape of a mature lakehouse catalog while preserving
workspace isolation for execution state.

## Scope Ownership

Metastore-scoped state:

- catalogs, schemas, tables, table formats, columns, constraints, and views
- volumes, functions, registered models, and model versions
- principals and group-membership snapshots used for metastore authorization
- grants, ownership, inherited permissions, and compiled authorization views
- storage credentials, external locations, managed roots, and governed paths
- credential vending policy, TTL clamps, deny reasons, and audit records
- governance attachments, policy placeholders, tags, glossary terms, and classifications
- table and object lineage bindings
- catalog, access, storage, governance, lineage, and Delta system projections

Workspace-scoped state:

- orchestration runs, tasks, sensors, schedules, backfills, and dispatch outbox
- runtime request context, workload identity, and request IDs
- workspace-local caches and temporary execution state
- workspace binding records when the binding object is optimized for workspace lookup
- workspace-local observability that does not define catalog authority

Table commit coordination for managed Delta tables is metastore/table scoped,
because table identity and commit success must be shared across every bound
workspace.

## Physical Layout Target

Current code uses `ScopedStorage` with paths shaped as:

```text
tenant={tenant}/workspace={workspace}/...
```

The target catalog authority should be able to use paths shaped as:

```text
tenant={tenant}/metastore={metastore}/
  manifests/
    root.manifest.json
    catalog.pointer.json
    lineage.pointer.json
    access.pointer.json
    storage.pointer.json
    governance.pointer.json
    delta.pointer.json
  locks/
  ledger/
    catalog/
    metastore/
    access/
    storage/
    governance/
    lineage/
    delta/
  snapshots/
    catalog/
    access/
    storage/
    governance/
    lineage/
    delta/
  state/
  quarantine/
  sequence/
```

Workspace execution state can remain workspace-scoped:

```text
tenant={tenant}/workspace={workspace}/
  manifests/
  ledger/orchestration/
  state/orchestration/
  locks/
```

Do not move all domains at once. The migration should introduce a scope
abstraction first, then route catalog authority through metastore-scoped storage
while preserving the existing workspace mapping for compatibility.

## Request Context

Every request that touches catalog state should carry:

```text
tenant_id
workspace_id
metastore_id
principal_id
group_snapshot_version
request_id
```

Authorization must validate both:

1. The principal has the object privilege.
2. The workspace is bound to the metastore or object being accessed.

This prevents a user with valid table privileges from using an unbound workspace
as an unintended access path.

## Binding Semantics

Workspace bindings are authoritative metastore objects. A binding can target:

- a whole metastore
- a catalog
- a schema
- a table, view, volume, function, or model
- an external location or managed root

The first tranche should support metastore-level bindings and object-level
bindings only where credential vending or path governance requires them.

Binding evaluation should deny closed when:

- the workspace has no active binding to the metastore
- the binding is deleted, disabled, stale, or outside its effective time window
- the compiled binding view is missing or stale for an enforcement route
- the request metastore does not match the object's owning metastore

## Read Path

For a request from `workspace=bi` reading `sales.curated.orders`:

1. Authenticate the principal and resolve group membership.
2. Resolve the workspace's metastore binding.
3. Resolve `sales.curated.orders` in the bound metastore.
4. Read the metastore's published catalog snapshot or object-native head.
5. Authorize against compiled grants and ownership using stable object IDs.
6. Apply storage governance and credential scope.
7. Return metadata, scan planning data, or scoped credentials.

Hot paths must use known-key reads and pointer-selected projections. Object
storage listing is reserved for repair, migration, and anti-entropy tools.

## Mutation Path

Metastore mutations append immutable events under the metastore scope, replay
into typed state, compact into redacted Parquet projections, and become visible
only after fenced pointer publication.

Workspace context is still recorded with each mutation for audit and policy
evaluation, but it is not the durability scope for shared catalog authority.

Successful visible mutations must satisfy Arco's existing consistency model:
readers see the old complete snapshot or the new complete snapshot, never a
half-published set.

## Credential Vending

Credential vending must combine three decisions:

1. Principal authorization on the object or path.
2. Workspace binding to the metastore or governed storage object.
3. Provider-specific scope and TTL limits.

The minted credential scope must be no broader than the authorized object/path
and no broader than the workspace binding permits. Deny decisions must be
auditable without exposing secret material or internal policy payloads.

## System Tables

System tables should expose both metastore and workspace dimensions where useful:

```text
system.catalog.tables
system.access.grants
system.access.compiled_permissions
system.storage.workspace_bindings
system.storage.external_locations
system.governance.attachments
```

Rows must be scoped by request tenant, authorized metastore, and workspace
binding before returning data. System tables remain read-only, redacted,
watermarked, and derived. They must not become the enforcement source of truth.

## Migration Strategy

Use a compatibility-first migration:

1. Add `metastore_id` to internal request and catalog domain contracts.
2. Default `metastore_id` to `workspace_id` where callers do not provide it.
3. Introduce a storage-scope abstraction that can produce workspace-scoped and
   metastore-scoped prefixes.
4. Add tests proving the alias mapping preserves existing paths.
5. Add workspace-binding objects and compiled binding views.
6. Move new metastore object families to metastore-scoped paths first.
7. Backfill existing catalog DDL state into metastore-scoped projections behind a
   migration flag.
8. Make compatibility adapters pass explicit `metastore_id`.
9. Require workspace binding checks for enforcement routes.
10. Retire the workspace-as-metastore alias only after migration tooling and
    rollback procedures exist.

## Open Questions

- Should principals be tenant-scoped globally or metastore-scoped with tenant
  identity federation as an input?
- Should one workspace bind to multiple metastores in the first public release,
  or should that be a later expansion?
- Are managed roots always metastore-scoped, or can they be workspace-local
  roots delegated by a metastore?
- Do system-table queries default to one active metastore, or can they union
  across all metastores bound to the workspace?
- What is the user-facing error model for "object exists in a metastore, but
  this workspace is not bound to it" under the existence-privacy policy?

## Implementation References

- metastore scope architecture plan
- `docs/adr/adr-005-storage-layout.md`
- `docs/adr/adr-034-fenced-head-published-control-plane-transactions.md`
- `docs/adr/adr-037-arco-catalog-product-surface.md`
- `docs/adr/adr-039-catalog-consistency-model.md`
- `proto/arco/catalog/v1/metastore.proto`
- `crates/arco-core/src/scoped_storage.rs`
- `crates/arco-catalog/src/metastore/`
