# Catalog Privilege Matrix

This page is the catalog product privilege contract. It names the operations
that code and tests must enforce. The first compiled grants subsystem exists
for generic securable fixtures and UC permission adapter responses. Full
writer-backed grant persistence, route-wide enforcement, audit, and
domain-specific storage or credential checks remain planned unless a row below
states otherwise.

## Principles

- Stable object IDs are enforcement keys.
- Names are mutable lookup aliases.
- Deny by default for unknown principals, unknown objects, stale compiled
  permissions, ambiguous paths, and credential-provider failures.
- Owner semantics, grant option, inherited grants, and group revision have
  focused generic compiler tests. Admin override behavior must be tested before
  any domain is marked `Implemented`.

## Securable Hierarchy

Privilege inheritance follows this hierarchy unless a later ADR explicitly
changes it:

```text
metastore
  catalog
    schema
      table | view | volume | function | registered model
        model version
  storage credential
    external location | managed root
  service credential
    external service connection
  share | provider | recipient | data product | business domain
```

Path-scoped requests also require storage-governance evidence. A catalog
permission alone is not sufficient to mint credentials or authorize raw path
access.

## Matrix

| Object | Operation | Required Privilege | Status |
|---|---|---|---|
| Catalog | create catalog | metastore admin or catalog create | Implemented for catalog DDL path; grant enforcement planned |
| Catalog | read/list | browse or ownership/inherited read | Partial: generic compiled grants support inheritance; route enforcement planned |
| Catalog | update/delete | owner or manage | Partial: owner/manage compiler semantics exist; route enforcement planned |
| Schema | create schema | catalog owner or schema create on parent catalog | Implemented for DDL path; grant enforcement planned |
| Schema | read/list/update/delete | owner or inherited manage/read | Partial: generic compiled grants support inheritance; route enforcement planned |
| Table | create table | schema owner or table create on parent schema | Implemented for DDL path; grant enforcement planned |
| Table | read metadata | select, browse, owner, or inherited read | Partial: generic compiled grants and UC permission adapter visibility exist |
| Table | write data/commit | modify or owner plus storage authorization | Planned |
| View | create/read/update/delete | table-like privileges on parent schema and referenced objects | Planned |
| Volume | create/read/write/delete | volume create/read/write/manage plus storage authorization | Planned |
| Storage credential | create/update/delete | metastore admin or credential manage | Partial for Arco-native `/storage-credentials` create/list/get metadata adapter; native privilege contract, provider secret integration, update/delete, and system-table exposure remain planned |
| Service credential | create/update/delete | metastore admin or service credential manage | Planned |
| External location | create/update/delete | credential use plus external location manage | Partial for `/external-locations` create/list/get adapter over scoped metastore storage-governance validation; update/delete, broader binding lifecycle, and native privilege contract remain planned |
| External service connection | create/update/delete | service credential use plus connection manage | Planned |
| Managed root | create/update/delete | metastore admin or storage manage | Planned |
| Function | create/read/execute/delete | function create, execute, manage | Planned |
| Registered model | create/read/update/delete | model create, read, manage | Planned |
| Model version | create/read/finalize/delete | model manage plus artifact storage authorization | Planned |
| Governance attachment | attach/detach/list | object manage or governance attach privilege | Planned |
| Share | create/read/update/delete | metastore admin, share manage, or owner | Planned |
| Provider / recipient | create/read/update/delete | metastore admin, sharing manage, or owner | Planned |
| Data product / business domain | create/read/update/delete | governance manage, domain owner, or metastore admin | Planned |
| System table | query | explicit system-table read scope and row-level redaction | Partial for catalog/lineage/orchestration allowlist |

## Canonical Operation Keys

Tests generated from this document should use the operation keys below. Adding
or renaming a key is a product-contract change.

| Operation key | Applies to | Required checks |
|---|---|---|
| `catalog.create` | Catalog | metastore-scoped actor, idempotency, audit, deny hidden parents |
| `catalog.read` | Catalog, Schema, Table, View, Volume, Function, Model | object ID authorization, hidden-object existence privacy, redacted fields |
| `catalog.list` | Catalog, Schema, Table, View, Volume, Function, Model | paginated result, deterministic order, hidden-object filtering, no count leakage |
| `catalog.update` | Catalog, Schema, Table, View, Volume, Function, Model | owner/manage privilege, ETag or generation where stale writes matter, field-mask validation |
| `catalog.delete` | Catalog, Schema, Table, View, Volume, Function, Model | owner/manage privilege, tombstone semantics, deny stale preconditions |
| `data.read` | Table, View, Volume, Model version | object privilege plus storage binding, credential scope subset, audit allow and deny |
| `data.write` | Table, Volume, Model version | modify/write privilege plus storage binding, credential scope subset, idempotency where applicable |
| `grant.read` | Any securable | Partial: UC adapter can expose compiled assignment rows; manage/grant visibility and redacted ordinary-principal views remain planned |
| `grant.update` | Any securable | Planned: UC `PATCH /permissions` remains unsupported until writer-backed persistence, grant option enforcement, and audit exist |
| `credential.metadata.create` | Storage credential, service credential | metastore/admin or manage privilege, secret material separated from safe metadata, request ID, audit allow and deny |
| `credential.metadata.read` | Storage credential, service credential | manage/use visibility, redacted secret handles, hidden-object existence privacy, audit for sensitive reads |
| `credential.metadata.list` | Storage credential, service credential | paginated result, workspace/metastore scope, hidden-object filtering, no secret or handle leakage |
| `credential.metadata.update` | Storage credential, service credential | manage privilege, field-mask validation, ETag or generation where stale writes matter, audit allow and deny |
| `credential.metadata.delete` | Storage credential, service credential | manage privilege, tombstone semantics, dependent-location safety checks, audit allow and deny |
| `storage.location.create` | External location, managed root | credential use/manage privilege, canonical path ownership, overlap rejection, workspace binding, audit allow and deny |
| `storage.location.read` | External location, managed root | location read/manage privilege, redacted credential references, hidden-path existence privacy |
| `storage.location.list` | External location, managed root | paginated result, deterministic order, workspace filter, no hidden-path or overlap-detail leakage |
| `storage.location.update` | External location, managed root | location manage privilege, path recanonicalization, overlap checks, ETag or generation where stale writes matter |
| `storage.location.delete` | External location, managed root | location manage privilege, tombstone semantics, dependent-object safety checks, audit allow and deny |
| `service.connection.manage` | External service connection | service credential use plus connection manage privilege, redacted endpoint/secret metadata, audit allow and deny |
| `governance.attachment.list` | Governance attachment | object read/manage or governance visibility privilege, redacted policy payloads, no hidden-object leakage |
| `governance.attachment.update` | Governance attachment | object manage or governance attach privilege, typed attachment validation, no implicit policy enforcement |
| `credential.mint` | Table, Volume, External location, Managed root, Model version, governed path | Partial for UC table/path credential decisions over compiled authorization and published storage governance; provider token material, revocation metadata, volume/model vending, and full native product-contract parity remain planned |
| `system.query` | System table | explicit system-table read scope, workspace filter, schema redaction, freshness watermark |
| `admin.explain_access` | Any securable | safe deny reason, grant evidence visibility, no hidden policy payloads |
| `sharing.manage` | Share, provider, recipient | stable sharing object ID, owner/admin privilege, redacted recipient/provider metadata, audit allow and deny |
| `governance.domain.manage` | Data product, business domain, glossary/domain metadata | stable domain ID, owner/governance privilege, controlled vocabulary checks, no hidden policy payloads |

## Required Deny Reasons

Authorization tests should assert stable deny reasons instead of matching free
text. The initial reason-code set is:

- `unknown_principal`
- `unknown_object`
- `permission_denied`
- `stale_permissions`
- `stale_projection`
- `ambiguous_path`
- `path_not_governed`
- `credential_provider_failed`
- `credential_scope_denied`
- `redacted`

## Test Requirements

Each implemented row needs tests for direct grants, inherited grants, owner
grants, group membership revision, rename stability, revoke behavior, deny
reason, audit event, and redacted response shape.
