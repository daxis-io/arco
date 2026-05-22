# Credential Vending Security

Credential vending is an authorization decision with provider-specific minting,
not a route-local response constructor.

## Current Implementation Status

The current UC-compatible implementation is `compatible-partial`:

- `POST /temporary-path-credentials` and `POST /temporary-table-credentials`
  use compiled authorization, published storage-governance state, path-scope
  checks, TTL clamping, allow/deny audit hooks, and redacted responses.
- Table credentials resolve the table through the native catalog before
  delegating to the governed-path decision path.
- `POST /temporary-volume-credentials` and
  `POST /temporary-model-version-credentials` remain known-but-unsupported
  operations.
- The UC response currently exposes the decision, reason, authorized prefixes,
  provider, credential kind, max TTL, audit event ID, and scoped credential
  metadata. Provider token material, revocation classification, and expiry
  timestamp exposure remain planned.

## Decision Input

Credential decisions include:

- principal ID and group snapshot version
- tenant and workspace
- request ID
- operation such as read, write, list, delete, model artifact read, or model
  artifact write
- object reference or governed path
- requested path and TTL
- client kind such as native, UC, Iceberg, SQL, or internal
- catalog snapshot version

## Decision Output

The full product contract is that credential decisions return:

- `allow` or `deny`
- stable reason code and safe reason message
- authorized object ID
- authorized path prefixes
- provider and credential kind
- expiry and applied max TTL
- revocation status and revocation-limit class
- audit event ID

Current UC-compatible responses implement the allow/deny, reason, path prefix,
provider, credential kind, max TTL, audit event ID, and redaction portions of
that contract. Expiry is calculated by the shared decision engine but is not yet
exposed in the UC response, and revocation status is still future provider
capability work.

## Requirements

- Deny by default.
- Clamp TTL to policy.
- Credential scope cannot exceed the governed table, volume, external location,
  managed root, model-version artifact location, or authorized path.
- Provider failure denies unless an explicit degraded mode is documented and
  tested.
- Provider revocation limitations must be explicit in the decision response and
  audit row. Non-revocable or best-effort-revocation credentials require short
  TTLs and cannot be silently upgraded to longer-lived scopes.
- Every allow and deny emits an audit event.
- Audit rows, system tables, logs, traces, errors, docs, and fixtures never
  contain raw tokens, secret material, encrypted payloads, or private keys.
- Stale permission or storage projections deny unless bounded staleness is
  explicitly accepted by the route.

## Testable Contract

Credential vending tests should assert the decision contract, not provider
implementation details.

| Area | Contract |
|---|---|
| TTL | Requests above policy maximum are clamped; requests below provider minimum fail or are raised only when documented; expired or zero TTL denies |
| Scope | Minted scope is a subset of the authorized table, volume, external location, managed root, model artifact location, or governed path |
| Path canonicalization | Dot segments, duplicate slashes, percent encoding, trailing slash variants, and provider-specific case rules cannot widen scope |
| Authorization | The route calls `AuthzDecision` before provider minting and records the group snapshot version |
| Provider failure | Provider errors deny closed with safe reason code `credential_provider_failed` and an audit row |
| Revocation | Credential responses report whether revocation is unsupported, best effort, or provider-confirmed; tests prove TTL policy tightens when revocation is limited |
| Audit | Every allow and deny records request ID, principal, operation, object ID or redacted path, decision, reason, TTL, provider, revocation status, and audit event ID |
| Redaction | Tokens, private keys, secret material, encrypted payloads, provider raw responses, and internal policy payloads never appear in errors, logs, traces, audit rows, system tables, fixtures, or OpenAPI examples |

## Scope Classes

The first implementation may support GCS table credentials, but the common
contract reserves these scope classes:

- `table_read`
- `table_write`
- `volume_read`
- `volume_write`
- `external_location_read`
- `external_location_write`
- `path_read`
- `path_write`
- `model_artifact_read`
- `model_artifact_write`

Path scopes are implemented for governed external-location and managed-root
authority. Table scopes are implemented by resolving the table location through
the native catalog and then applying the governed-path decision path. Volume and
model-artifact scopes stay `planned` until the corresponding authoritative
object ownership exists.

## Provider Extensibility

GCS can be the first provider, but the interface must leave room for S3, Azure,
remote signing, access-delegation negotiation, and revocation-limit reporting
without weakening the common decision contract.

Provider capabilities are negotiated as named features such as
`temporary_token`, `remote_signing`, `downscoped_token`, and
`revocation_status`. Missing capabilities deny unless the route declares a safe
fallback in this document and has tests for that fallback.

Revocation status is a provider capability, not a security assumption. A
credential response must classify revocation as one of:

- `provider_confirmed`: provider exposes a revocation or invalidation mechanism
  that Arco can call and audit.
- `best_effort`: Arco can revoke or rotate an upstream binding, but already
  minted credentials may remain valid until expiry.
- `unsupported`: Arco cannot revoke the minted credential before expiry.

Routes that allow `best_effort` or `unsupported` revocation must clamp TTL to the
shortest policy maximum for the object family and operation, include the
revocation class in the safe response metadata, and audit the limitation without
logging credential material. A route that requires revocation must deny with a
stable safe reason when the provider only offers weaker revocation.

## Redaction

Secret exclusion should happen by type and schema. Route filtering is only a
secondary guard. Tests must prove list/get responses, system tables, audit
projections, OpenAPI examples, and error messages do not expose credential
payloads.
