# ADR-031: Unity Catalog OSS API Facade (UC Parity)

## Status

Proposed

## Context

Arco targets **Unity Catalog (UC) compatibility** primarily as an interoperability
contract for Delta engines and clients that expect UC-style APIs.

Constraints:

1. **File-native control plane**: correctness-critical catalog state remains in
   queryable Parquet snapshots and small control-plane objects (Tier-1 semantics).
2. **Compactor sole writer**: API servers do not directly mutate Parquet state; they
   append events / acquire locks and trigger compaction (ADR-018).
3. **No correctness-critical listing**: hot paths must use known-key `GET`/`HEAD`
   only; listing is reserved for explicit anti-entropy tools (ADR-019).
4. **Pinned contract**: UC OSS OpenAPI (`api/all.yaml`) is vendored and pinned and is
   the source of truth for endpoint surface area.

Arco already serves:
- `/api/v1/*` (Arco native API surface)
- `/iceberg/v1/*` (Iceberg REST Catalog)

We need a UC facade that:
- matches the pinned OpenAPI surface
- enforces tenant/workspace scoping on every request
- preserves Arco invariants and deployment posture guardrails

## Decision

**Introduce a dedicated UC facade crate (`crates/arco-uc`) and mount it in `arco-api`
behind a config flag.**

### Routing and mounting

- `arco-uc` owns an Axum `Router` and its own request context and error model.
- `arco-api` mounts the UC router when `unity_catalog.enabled = true`.
- Default mount path is `/api/2.1/unity-catalog`, matching the Unity Catalog OSS
  OpenAPI `servers.url` base path. This is configurable, but parity testing assumes
  the pinned OpenAPI base path.

### Request context and scoping

- `arco-api` remains the source of truth for auth (JWT / debug header extraction).
- A UC-specific middleware translates the existing `RequestContext` into a
  `UnityCatalogRequestContext` (tenant, workspace, request_id, idempotency key).
- Every UC handler requires this request context (except explicitly public endpoints
  specified by the pinned OpenAPI, e.g. an OpenAPI document endpoint).

### Authority model

- UC "catalog objects" (catalogs/schemas/tables) and compiled grants are persisted as
  **Tier-1** state using `arco_catalog::{CatalogReader, CatalogWriter}` (via sync
  compaction where enabled).
- Operational metadata that can be eventual (e.g., certain audit receipts, optional
  projections) is stored as events and compacted.

### Error model

- UC handlers return **spec-shaped** JSON errors (payload shape and status codes
  aligned to the pinned OpenAPI).
- Responses include `x-request-id` when available for debuggability.

### OpenAPI

- `arco-uc` generates an OpenAPI spec using `utoipa`.
- A contract test compares the generated spec to the vendored pinned spec to enforce
  parity on paths/methods/parameter names/response codes at minimum.

## Test Strategy

1. **Contract tests (required)**
   - `arco-uc` OpenAPI compliance: vendored pinned spec is a subset of generated spec.
2. **Integration tests (required)**
   - In-process Axum server smoke tests for tenant/workspace scoping and core flows.
3. **Failure-mode tests (required)**
   - Delta commit coordination conflicts and crash recovery paths.
   - Credential vending TTL clamps and deny-audit emission.

## Consequences

- UC compatibility becomes an explicit, testable contract: the pinned OpenAPI spec is
  the single parity reference.
- The UC facade is isolated from Arco native APIs, reducing coupling and allowing
  UC-specific error handling and semantics.
- Shipping parity requires vendoring and pinning `api/all.yaml` (network-free builds)
  and maintaining compliance tests as the primary drift detector.
