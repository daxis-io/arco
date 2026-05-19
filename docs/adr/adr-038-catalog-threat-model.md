# ADR-038: Catalog Threat Model

## Status

Proposed

## Context

The catalog product surface will handle securable object identity, grants,
credentials, governed paths, system tables, lineage, and audit data. These are
security-sensitive control-plane functions. Before implementation expands, the
threat model must name the risks that tests, route behavior, projections, and
docs are expected to mitigate.

## Decision

Arco uses the following STRIDE-oriented threat model for catalog product work.

| Category | Threat | Required Mitigation |
|---|---|---|
| Spoofing | Caller forges user, service, workload, group, workspace, or request identity | Authenticate every route, record request ID, bind decisions to principal and group snapshot, deny unknown or stale identity by default |
| Spoofing | Lineage event claims a false producer, consumer, job, or run | Validate event source, preserve event IDs, dedupe deterministically, expose freshness and provenance in derived views |
| Tampering | Caller swaps object names or IDs to access another securable | Authorize by stable object ID, treat names as aliases, test rename stability |
| Tampering | Caller exploits path normalization gaps or external location overlap | Canonicalize provider paths, reject unsafe overlap, test S3/GCS/Azure/local cases |
| Tampering | Compatibility route accepts a caller-supplied table URI, path, or name that conflicts with catalog state | Resolve through native object ID and catalog-bound storage state before coordinator, credential, or mutation work |
| Tampering | Projection files are stale, corrupt, partial, or replay-inconsistent | Use immutable snapshots, pointer publication, schema checks, replay-equivalence checks, and freshness watermarks |
| Repudiation | Mutations, denies, or credential mints lack durable audit evidence | Emit audit events for allow and deny decisions, mutations, credential vending, and admin overrides |
| Information disclosure | Secret material appears in projections, system tables, logs, traces, errors, fixtures, or OpenAPI examples | Exclude secrets by type/schema, redact error responses, add redaction tests and fixture checks |
| Information disclosure | System tables reveal hidden objects, raw paths, grant internals, or policy payloads | Explicit allowlist, workspace scoping, response redaction classes, and safe error codes |
| Denial of service | Expensive search, list, permission compilation, query, or credential routes exhaust resources | Pagination, max page sizes, timeouts, quotas, row limits, and rate limits |
| Elevation of privilege | Inherited grants, owner grants, stale group membership, or grant option semantics are wrong | Compile permissions from authoritative state, test inheritance, group revision, owner transfer, grant/revoke, and deny reasons |
| Elevation of privilege | Credential scope exceeds authorized object or TTL policy | Clamp TTL, derive path prefixes from authorized objects, audit all decisions, deny provider failures by default |
| Elevation of privilege | Sensitive system-table data is used as an enforcement shortcut | Keep system tables read-only and derived; enforcement uses published authoritative state or compiled views |

## Consequences

- Every endpoint accepting object IDs, names, paths, principals, operations, or
  field masks needs object-level authorization tests.
- Every response schema needs property-level authorization and redaction tests.
- Credential vending is an authorization decision with provider plugins, not a
  route-local response builder.
- System tables and discovery surfaces must be safe to expose to ordinary
  principals without leaking hidden objects or sensitive internals.
- Deny-path tests must assert stable reason codes so future changes cannot
  replace fail-closed behavior with ad hoc errors.
