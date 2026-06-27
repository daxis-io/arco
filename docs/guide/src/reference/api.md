# API Reference

## Canonical Contract

The canonical REST contract snapshot is:

- `crates/arco-api/openapi.json`

Any API-facing change should update this file as part of the same pull request.

## Service Implementation

Primary API composition and routing live in:

- `crates/arco-api/src/server.rs`
- `crates/arco-api/src/routes/`

## Auth and Request Context

Authentication and request-context extraction are implemented in:

- `crates/arco-api/src/config.rs`
- `crates/arco-api/src/context.rs`

## API Change Policy

When changing public API behavior:

1. Update code and tests.
2. Update `crates/arco-api/openapi.json`.
3. Update relevant mdBook docs under `docs/guide/src/`.
4. Include migration notes in PR/release notes for breaking or behavior-sensitive changes.

## Public Rust Enum Compatibility Policy

Public error enums exposed by stable crates are non-exhaustive so downstream Rust callers keep a wildcard arm and Arco can add precise variants before 1.0 without making every exhaustive match a source break. Public contract validation enums follow the same rule. The current public Rust enum inventory covered by this policy is `arco_core::Error`, `arco_catalog::CatalogError`, `arco_delta::DeltaError`, `arco_iceberg::IcebergError`, `arco_uc::UnityCatalogError`, `arco_proto::TaskOutputContractError`, `arco_proto::ControlPlaneTransactionContractError`, `arco_proto::CatalogDdlContractError`, `arco_proto::MetastoreMutationContractError`, `arco_proto::CatalogControlPlaneScopeContractError`, `arco_proto::OrchestrationEventContractError`, and `arco_proto::WorkerCallbackContractError`.

Protocol and wire-format enums may remain exhaustive when their variant set is the compatibility contract itself. Those enums require explicit versioning, OpenAPI/protobuf/serialization coverage, and migration notes before variants are removed or renamed. Adding a wire variant is allowed only when old readers can reject, ignore, or quarantine it intentionally.

Internal controller, state-machine, and test-only enums are not automatically `#[non_exhaustive]`; they follow normal Rust exhaustiveness unless they are re-exported as stable downstream APIs.

## JSON Casing Policy

Existing `/api/v1` catalog, namespace, table, and Delta route families keep their snake_case JSON fields for compatibility with released clients. New fields added to those response families use the same snake_case convention unless a versioned migration is approved.

New native `/api/v1` route families use camelCase JSON fields. Compatibility adapters may expose protocol-native casing when that adapter contract requires it, but the route documentation and OpenAPI schema must pin the casing explicitly.

Changing an existing route family from snake_case to camelCase is a breaking API change. It requires a documented migration path, a versioned route or media-type boundary, updated client fixtures, and release notes before implementation.

## Idempotency Policy

`Idempotency-Key` is the transport-level retry key for public write routes that document it. Reusing a key with the same semantic payload returns the original accepted resource or idempotent success; reusing it with a different semantic payload returns `409 CONFLICT`.

Trigger-run accepts `runKey` first and falls back to `Idempotency-Key` when the request body omits `runKey`. The effective run key is validated before any reservation is written.

Backfill creation accepts `clientRequestId` first and falls back to `Idempotency-Key`. One of those values is required, and the chosen value owns the backfill replay/conflict record.

Manifest deployment uses `Idempotency-Key` when present. Matching manifest payloads replay the existing manifest response; mismatched payloads return `409 CONFLICT`. Omitting the header keeps the existing non-idempotent deploy behavior.

Schedule mutations use `Idempotency-Key` as the event idempotency discriminator when present. The discriminator is operation-specific. Without the header, the generated event id is used so ordinary distinct updates remain distinct. Equivalent schedule state is still returned idempotently from the current folded state; reusing the same operation key for a different schedule mutation returns `409 CONFLICT`.
