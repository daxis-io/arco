# UC Compatibility Support Registry Implementation Plan

**Goal:** Make Arco's embedded Unity Catalog support boundary explicit, machine-checkable, and stable for downstream embedders tracking issue #130.

**Architecture:** Add a first-class route support registry in `arco-uc` that is the source of truth for implemented, compatible-partial, known-unsupported, and planned UC operations. Wire router fallback, generated OpenAPI metadata, docs, and tests to that registry so known UC gaps return structured `501` responses instead of ambiguous `404`s, while non-UC unknown paths remain `404`.

**Tech Stack:** Rust, Axum, utoipa/OpenAPI, serde/serde_json, mdBook, cargo tests.

---

## Strategy

This patch is a compatibility-contract slice, not a broad UC parity expansion.

In scope:

- Route-level support metadata for the currently mounted `arco-uc` surface and the pinned UC operations most likely to affect embedders.
- Router fallback behavior that distinguishes unknown paths from known-but-unsupported UC operations.
- Generated OpenAPI vendor extensions that expose Arco's support level and backing boundary for documented operations.
- Focused tests that prevent router, registry, docs, and OpenAPI support labels from drifting apart.
- Documentation updates that point embedders at the registry-backed contract.

Out of scope:

- Implementing all Unity Catalog endpoint families.
- Moving Daxis/product policy authoring into Arco.
- Trusting client-supplied principal headers.
- Exposing `system.access.*` or `system.storage.*` tables before projection-backed serving exists.
- Claiming `PATCH /permissions`, volume/model credentials, provider token material, revocation metadata, or full route-wide grant enforcement as complete.

## Task 1: Add Failing Registry Contract Tests

**Files:**

- Create: `crates/arco-uc/tests/support_registry.rs`
- Modify: `crates/arco-uc/tests/openapi_compliance.rs`

**Step 1: Test router fallback semantics**

Add tests proving:

- `PATCH /permissions/table/main.default.t1` returns `501 NOT_IMPLEMENTED` with error code `NOT_SUPPORTED`.
- A known pinned UC route that is not explicitly mounted, such as `GET /volumes/main/default/v1`, returns the same structured `501`.
- A non-UC route, such as `GET /definitely-not-in-spec`, still returns `404 NOT_FOUND`.

**Step 2: Test registry classifications**

Add tests proving:

- `GET /catalogs` is `implemented`.
- `POST /temporary-table-credentials` is `compatible-partial`.
- `PATCH /permissions/{securable_type}/{full_name}` is `known-unsupported`.
- `GET /volumes/{full_name}` is `planned`.
- Every registry entry references a route that is present in the pinned UC fixture unless it is explicitly marked as Arco-native.

**Step 3: Test OpenAPI support metadata**

Extend `openapi_compliance.rs` so every generated operation that is part of the UC facade has:

- `x-arco-support-level`
- `x-arco-native-backing`
- `x-arco-authz-boundary`

Expected before implementation: these tests fail because no registry API or OpenAPI extensions exist yet.

## Task 2: Implement the UC Support Registry

**Files:**

- Create: `crates/arco-uc/src/support.rs`
- Modify: `crates/arco-uc/src/lib.rs`
- Modify: `crates/arco-uc/src/contract.rs`

**Step 1: Add public registry types**

Implement:

- `SupportLevel::{Implemented, CompatiblePartial, KnownUnsupported, Planned}`
- `UcOperationSupport`
- `UcSupportMatch`
- `operation_support(method, path)`
- `documented_operations()`

Each operation entry should include:

- method
- path template
- support level
- route group
- native backing description
- authz boundary description
- known gap description

**Step 2: Reuse existing pinned-spec matching**

Move or expose the existing path-template matching in `contract.rs` so the registry can:

- match parameterized paths reliably
- preserve `/api/2.1/unity-catalog` prefix stripping
- confirm registry entries map to the pinned UC fixture

**Step 3: Keep classifications conservative**

Initial labels:

- Catalog/schema/table CRUD: `implemented`
- Delta commit preview routes: `compatible-partial`
- Permissions `GET`: `compatible-partial`
- Permissions `PATCH`: `known-unsupported`
- Storage credentials create/list/get: `compatible-partial`
- External locations create/list/get: `compatible-partial`
- Temporary table/path credentials: `compatible-partial`
- Temporary volume/model credentials: `known-unsupported`
- Volumes/functions/models route families: `planned`

## Task 3: Wire Router Fallback to Registry

**Files:**

- Modify: `crates/arco-uc/src/router.rs`
- Modify: `crates/arco-uc/src/routes/common.rs`

**Step 1: Add registry-backed fallback**

Change fallback behavior:

- If the request method/path matches a registry entry with `known-unsupported` or `planned`, return `501`.
- If the request method/path is in the pinned UC fixture but has no registry entry yet, return `501` with a message identifying it as a known UC operation outside Arco's current support contract.
- Otherwise return `404`.

**Step 2: Enrich unsupported messages**

Make unsupported messages include:

- request method and path
- support level
- route group
- known gap

Do not change the existing UC error envelope or error code without a separate API-compatibility decision.

## Task 4: Add OpenAPI Support Extensions

**Files:**

- Modify: `crates/arco-uc/src/openapi.rs`
- Modify: `crates/arco-uc/tests/openapi_compliance.rs`

**Step 1: Decorate generated operations**

After `UnityCatalogApiDoc::openapi()` generation, inject vendor extensions into generated UC operations based on the registry:

- `x-arco-support-level`
- `x-arco-native-backing`
- `x-arco-authz-boundary`
- `x-arco-known-gap` when present

**Step 2: Keep OpenAPI path coverage honest**

Do not add planned/unsupported operations to generated OpenAPI unless they have documented request/response behavior. The registry can know more than OpenAPI exposes; OpenAPI should remain the documented callable surface.

## Task 5: Update Docs

**Files:**

- Modify: `docs/guide/src/reference/unity-catalog-openapi-inventory.md`
- Modify: `docs/guide/src/reference/catalog-api-contract.md`
- Modify: `docs/guide/src/reference/control-plane-scope.md`

**Step 1: Point embedders at the registry-backed contract**

Document that route support status is now checked in code through `arco_uc::support`.

**Step 2: Clarify fallback semantics**

Document:

- mounted supported or partial routes execute normally
- known unsupported/planned UC operations return structured `501`
- unknown non-UC paths return `404`

**Step 3: Preserve governance non-goals**

Keep language explicit that writer-backed grant mutation, route-wide authz enforcement, system tables, and provider credential material remain future work.

## Task 6: Verification and Review

**Files:**

- All changed files

**Step 1: Run focused tests**

Run:

```bash
cargo test -p arco-uc --test support_registry -- --nocapture
cargo test -p arco-uc --test openapi_compliance -- --nocapture
cargo test -p arco-uc --test preview_crud --test permissions_authoritative --test storage_governance_authoritative --test credentials_authoritative -- --nocapture
```

**Step 2: Run docs and formatting gates**

Run:

```bash
cargo fmt --check
cd docs/guide && mdbook build
git diff --check
```

**Step 3: Review**

Perform a senior review of the diff against this plan:

- no full-UC parity creep
- no client-header trust
- no overclaiming governance
- no route without support metadata
- no unsupported known UC operation returning ambiguous `404`
