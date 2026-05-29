# Catalog API Contract

This page defines native catalog API behavior and compatibility-adapter
requirements for future catalog product work.

## Resource Identity

- Stable IDs are the canonical resource identifiers.
- Names are mutable aliases and may be accepted by compatibility adapters.
- Full names must resolve to stable IDs before authorization.
- Rename preserves object ID, grants, lineage bindings, governance attachments,
  storage binding, and audit identity.

## Request Shape

- Every request has tenant, workspace, principal, request ID, and authenticated
  context.
- Mutating APIs accept idempotency keys when retries can duplicate work.
- Update and delete APIs use ETags, generations, or explicit preconditions where
  stale writes matter.
- Update APIs use field masks instead of ambiguous partial-update behavior.

## Native Resource Contract

Native resource names use stable IDs in request paths whenever the operation is
an enforcement point. Compatibility adapters may accept external full-name
forms, but they must resolve the name to a stable ID before authorization,
storage binding, credential vending, mutation, or audit.

New native mutating endpoints must declare:

- the stable object ID field used for authorization
- the parent object ID or governed path used for inherited authorization
- whether the request requires an idempotency key
- whether the request requires an ETag, generation, read version, or other
  precondition
- which response fields are ordinary, owner, or admin-visible

## Table Format Contract

Arco is a catalog and metastore for open lakehouse table formats. Native table
contracts recognize Delta Lake, Iceberg, and plain Parquet as explicit table
format values.

New Arco table registrations default to Delta Lake when the caller omits a
format. That default is separate from legacy read behavior: older table rows
without persisted format metadata are interpreted as plain Parquet so existing
metadata remains readable.

Compatibility adapters must preserve this distinction. Delta-specific commit
coordination is available only for Delta tables. Iceberg-facing routes and
plain Parquet table metadata are support-level-bound surfaces over Arco-owned
catalog state, not separate metastore implementations.

## List, Filter, And Sort

- List APIs are paginated from first release with `page_size`, `page_token`,
  deterministic ordering, and maximum page size.
- Filters and sorts use explicit allowlists. Raw SQL-like filter strings are
  not part of the native API contract.
- List responses must not reveal hidden objects through counts, errors, timing,
  or pagination tokens.

Every new list endpoint must have a default page size, a maximum page size, a
stable sort key, an opaque page-token format, and deny-path tests for hidden
objects. An unpaginated list endpoint is a contract violation unless the route
is internal-only and explicitly documented outside the public API.

Filter and sort allowlists are part of the route contract. Adding a filter or
sort key is compatible only when the field is visible at the caller's response
class and has tests for hidden or redacted objects.

## Errors

Native error responses use stable machine-readable codes:

- `not_found`
- `permission_denied`
- `conflict`
- `precondition_failed`
- `invalid_path`
- `stale_projection`
- `credential_scope_denied`
- `redacted`
- `unsupported`

Native APIs use a problem-details shape:

```text
code
message
request_id
resource_type
resource_id | redacted_resource
retryable
details[]
```

Error messages must be safe for ordinary principals. Raw paths, raw tokens,
secret payloads, hidden object names, and internal policy payloads are never
returned. Compatibility adapters may preserve an external error schema, but
they must map to one of the native reason codes internally and preserve request
ID propagation.

## Response Visibility

Response fields are classified as admin-visible, owner-visible, or
ordinary-principal-visible. Secret material, raw credential payloads, private
keys, raw tokens, and internal policy payloads are never visible in list, get,
system-table, discovery, logs, traces, or examples.

Schemas must declare the visibility class of fields that can expose paths,
principals, grant evidence, provider metadata, policy attachment state, audit
references, lineage provenance, or credential decisions. Tests must cover at
least one ordinary-principal, owner, and admin response for every sensitive
route family before the route is marked production-backed.

## Compatibility Labels

Every public route group must have one documented compatibility label. Unity
Catalog compatibility routes use the route-level `arco_uc::support` registry
with these labels: `implemented`, `compatible-partial`,
`known-unsupported`, and `planned`. Native Arco-only surfaces may still use
`native` when they are not compatibility adapters.

The generated OpenAPI carries per-operation support metadata for documented UC
operations:

- `x-arco-support-level`
- `x-arco-native-backing`
- `x-arco-authz-boundary`
- `x-arco-known-gap` when a gap is known

Known unsupported or planned UC operations must return a structured `501`
instead of an ambiguous `404`. Unknown non-UC paths remain `404`.

## Versioning Gates

- Protobuf changes follow `docs/guide/src/reference/schema-evolution-policy.md`
  and must pass the proto breaking-change gate.
- OpenAPI changes require a checked-in diff or snapshot update with the route
  compatibility label.
- System-table schemas are public API once allowlisted and need golden-schema
  evidence.
- A compatibility adapter cannot be promoted beyond `compatible-partial`
  without request/response fixtures, deny-path fixtures, and redaction
  fixtures.

## Contract Checklist

Before a new public route is merged, reviewers should be able to identify:

| Contract item | Required evidence |
|---|---|
| Stable identity | Object IDs are authorization keys and names resolve before enforcement |
| Pagination | List endpoints have `page_size`, `page_token`, maximum size, and deterministic ordering |
| Preconditions | Mutating retries and stale writes use idempotency keys, ETags, generations, or explicit protocol tokens |
| Errors | Responses use stable codes, request IDs, retryability, and safe messages |
| Visibility | Sensitive fields have ordinary, owner, and admin response tests |
| Compatibility | Route group has one compatibility label and documented unsupported behavior |
