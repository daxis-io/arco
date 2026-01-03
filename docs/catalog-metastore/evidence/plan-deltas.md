# Plan Deltas After Security/Ops + Parity Review

**Date:** 2026-01-02  
**Commit:** 966b8eecce6348821c258e38553ed945e314cfde  
**Inputs:**
- `docs/catalog-metastore/evidence/security-ops-evidence-pack.md`
- Parity/PRD working docs (local, not in repo):
  - `Iceberg-Native Feature Parity, Architecture, and Delivery Plan.md`
  - `arco-catalog-gap-analysis.md`
  - `arco-catalog-feature-parity.md`
  - `arco_catalog_prd_tech_spec_parity_checklist.md`

This document captures the delta between “current state” and parity/security requirements after producing the evidence pack and cross-checking the parity/PRD documents.

---

## 1) What Changed After Review

Key findings that directly affect the delivery plan:

- The architecture treats **multi-tenancy**, **secrets never in events/logs**, and **audit trail covers all mutations** as completeness criteria (`docs/plans/2025-01-12-arco-unified-platform-design.md:4130`, `docs/plans/2025-01-12-arco-unified-platform-design.md:4131`, `docs/plans/2025-01-12-arco-unified-platform-design.md:4132`).
- Runtime posture contract implemented: `Posture` is derived from required `ARCO_ENVIRONMENT` + `ARCO_API_PUBLIC`, and Terraform wires `ARCO_API_PUBLIC` (`crates/arco-api/src/config.rs:10`, `crates/arco-api/src/config.rs:343`, `infra/terraform/cloud_run.tf:119`, `infra/terraform/cloud_run.tf:125`).
- Debug-mode (`ARCO_DEBUG=true`) is a tenant isolation bypass for API requests and task callbacks, but header-based scoping now requires `debug && posture.is_dev()` and startup rejects debug outside dev (`crates/arco-api/src/context.rs:80`, `crates/arco-api/src/routes/tasks.rs:602`, `crates/arco-api/src/server.rs:572`).
- JWT verification supports HS256/RS256, and issuer/audience enforcement exists but is optional (`crates/arco-api/src/context.rs:188`, `crates/arco-api/src/context.rs:123`). Terraform defaults `ARCO_JWT_ISSUER`/`ARCO_JWT_AUDIENCE` to empty strings (`infra/terraform/variables.tf:145`), and empty env vars are treated as “unset” (`crates/arco-api/src/config.rs:302`).
- `/metrics` is mounted without auth (`crates/arco-api/src/server.rs:379`). If `api_public=true` allows unauthenticated invocation (`infra/terraform/iam.tf:135`), `/metrics` becomes both an info disclosure surface and an attacker-amplifiable DoS vector.
- Metrics cardinality is attacker-amplifiable:
  - `endpoint` label falls back to raw `request.uri().path()` when `MatchedPath` is absent (`crates/arco-api/src/metrics.rs:97`).
  - Several counters label by raw tenant/workspace (`crates/arco-api/src/metrics.rs:181`, `crates/arco-iceberg/src/metrics.rs:216`). Hashing does not reduce series count; the label policy must reduce dimensions.
- Storage layer scoping is strong (`ScopedStorage`) and defends against traversal and cross-scope reads (`crates/arco-core/src/scoped_storage.rs:115`, `crates/arco-core/src/scoped_storage.rs:821`).
- Signed URL minting is manifest-allowlisted, tenant-scoped, and TTL-bounded (`crates/arco-api/src/routes/browser.rs:165`, `crates/arco-api/src/routes/browser.rs:127`, `crates/arco-catalog/src/reader.rs:494`, `crates/arco-catalog/src/reader.rs:601`), but lacks the *design-intended audit record at mint time* (`crates/arco-api/src/routes/browser.rs:143`, `docs/adr/adr-019-existence-privacy.md:101`).
- Catalog mutations are attributed as `api:{tenant}` rather than the authenticated principal identity (`crates/arco-api/src/routes/namespaces.rs:115`, `crates/arco-api/src/routes/tables.rs:170`), reducing forensic/audit value even if durable audit logs are added.
- Iceberg REST is mounted and protected by outer middleware (`crates/arco-api/src/server.rs:394`, `crates/arco-api/src/server.rs:402`), but the current Iceberg router only implements a Phase A subset + `commit_table` (`crates/arco-iceberg/src/router.rs:37`, `crates/arco-iceberg/src/types/config.rs:8`).
- Iceberg credential vending is **not wired end-to-end** (provider not attached) even though the handler requires one (`crates/arco-iceberg/src/state.rs:58`, `crates/arco-api/src/server.rs:397`, `crates/arco-iceberg/src/routes/tables.rs:578`).
- “OpenAPI compliance” and integration tests are necessary but not sufficient today:
  - The OpenAPI compliance test checks parameter-name and numeric status-code subsets, not full schema/behavior (`crates/arco-iceberg/tests/openapi_compliance.rs:107`).
  - The Iceberg integration test exercises the Iceberg router directly using custom tenancy headers, not the API-layer JWT auth model (`crates/arco-integration-tests/tests/iceberg_rest_catalog.rs:132`).

---

## 2) Parity/PRD Alignment (P0/P1 Highlights)

This is a minimal traceability slice for the metastore/security-critical items.

| Requirement (parity/PRD) | Priority | Status | Repo evidence / notes |
|---|---:|---|---|
| Iceberg REST: `GET /v1/config` and endpoint advertisement | P0 | ✅ | `crates/arco-iceberg/src/router.rs:43`, `crates/arco-iceberg/src/routes/config.rs:30`, `crates/arco-iceberg/src/types/config.rs:46` |
| Iceberg REST: namespaces list/get/HEAD + pagination | P0 | ✅ | `crates/arco-iceberg/src/routes/namespaces.rs:30`, `crates/arco-iceberg/src/routes/namespaces.rs:71`, `crates/arco-iceberg/src/routes/namespaces.rs:98` |
| Iceberg REST: tables list/load/HEAD + pagination | P0 | ✅ | `crates/arco-iceberg/src/routes/tables.rs:39`, `crates/arco-iceberg/src/routes/tables.rs:87`, `crates/arco-iceberg/src/routes/tables.rs:111` |
| Iceberg REST: load-table ETag + `If-None-Match` + 304 | P0 | ✅ | `crates/arco-iceberg/src/routes/tables.rs:189`, `crates/arco-iceberg/src/routes/tables.rs:193`, `crates/arco-iceberg/src/routes/tables.rs:269` |
| Iceberg REST: commit-table (`requirements` + `updates`) + idempotency key | P0 | ✅ | `crates/arco-iceberg/src/routes/tables.rs:333`, `crates/arco-iceberg/src/routes/tables.rs:406`, `crates/arco-iceberg/src/types/commit.rs:20`, `crates/arco-iceberg/src/types/commit.rs:78` |
| Iceberg REST: namespace create/delete/properties | P0 | ❌ | Not routed; write endpoints are explicitly disabled (`crates/arco-iceberg/src/types/config.rs:10`, `crates/arco-iceberg/src/routes/namespaces.rs:30`) |
| Iceberg REST: table create/drop/register/rename/metrics/transactions | P0/P1 | ❌ | Not routed; only `commit_table` is treated as supported write endpoint (`crates/arco-iceberg/src/router.rs:37`, `crates/arco-iceberg/src/types/config.rs:63`) |
| Iceberg REST: spec-declared params are actually parsed | P0 | ⚠️ | `snapshots` is declared but not currently parsed in handler signature (`crates/arco-iceberg/src/routes/tables.rs:128`, `crates/arco-iceberg/src/routes/tables.rs:146`) |
| Iceberg REST: credential vending endpoint is wired and audited | P0 | 🔨 | Endpoint exists; provider is optional and currently not wired (`crates/arco-iceberg/src/state.rs:58`, `crates/arco-api/src/server.rs:397`, `crates/arco-iceberg/src/routes/tables.rs:578`) |
| Browser read path: signed URL minting allowlisted + tenant-scoped + TTL bounded | P0 | ✅ | `crates/arco-api/src/routes/browser.rs:119`, `crates/arco-api/src/routes/browser.rs:165`, `crates/arco-catalog/src/reader.rs:494`, `crates/arco-catalog/src/reader.rs:601` |
| API auth: debug headers for dev posture vs Bearer JWT for non-dev | P0 | ⚠️ | `crates/arco-api/src/context.rs:80`, `crates/arco-api/src/context.rs:94`, `crates/arco-api/src/context.rs:292` |
| Security audit trail for decisions + mutations | P0 | ❌ | No structured security decision audit events (allow/deny) evidenced; note Tier-1 commit records provide tamper-evident mutation history but lack principal attribution/decision logging (`crates/arco-catalog/src/manifest.rs:738`, `crates/arco-core/src/storage_traits.rs:186`). URL minting currently logs safe metadata + metrics only (`crates/arco-api/src/routes/browser.rs:143`, `crates/arco-api/src/routes/browser.rs:181`) |
| Engine interop: Spark/Flink/Trino “MUST PASS” matrix + known-good configs | P0 | ❌ | Not present in repo; the existing Iceberg test uses custom headers (`crates/arco-integration-tests/tests/iceberg_rest_catalog.rs:132`) |

---

## 3) Updated P0 Blockers

### 3.1 Security/Ops (P0)

1. ✅ Implemented explicit runtime posture contract via required `ARCO_ENVIRONMENT` + `ARCO_API_PUBLIC` (`crates/arco-api/src/config.rs:343`, `infra/terraform/cloud_run.tf:119`, `infra/terraform/cloud_run.tf:125`).
2. ✅ Prevent `ARCO_DEBUG=true` outside dev posture at startup, and require `debug && posture.is_dev()` for header-based scoping in API + task callbacks (`crates/arco-api/src/server.rs:572`, `crates/arco-api/src/context.rs:80`, `crates/arco-api/src/routes/tasks.rs:602`). Tests cover dev accept + non-dev reject (`crates/arco-api/src/context.rs:292`, `crates/arco-api/src/context.rs:315`, `crates/arco-api/src/routes/tasks.rs:1234`, `crates/arco-api/src/routes/tasks.rs:1273`).
3. Require JWT `iss` + `aud` policy in production posture; fail closed when unset/empty.
4. Protect `/metrics` for public deployments and define a metrics policy that prevents identifier leakage and cardinality amplification (remove attacker-controlled label values; reduce tenant/workspace label dimensions).
5. Implement a minimal security/mutation audit event stream (auth allow/deny, URL mint allow/deny, credential vend allow/deny, Iceberg commit) consistent with the audit trail invariant.
6. Decide and document audit failure semantics (must-write vs best-effort) and DoS controls/sampling for deny-events.
7. Enforce bucket public access prevention in Terraform (`public_access_prevention = "enforced"`) and validate conditional-IAM list semantics for anti-entropy.

### 3.2 Iceberg Parity / Engine Readiness (P0)

8. Fix “spec says it exists but handler can’t read it” issues (query params, capability advertisement truthfulness) before adding new endpoints.
9. Implement missing Iceberg REST baseline endpoints: namespaces CRUD + properties, tables create/drop/register/rename, metrics POST, transactions commit, and required semantics (e.g., `purgeRequested`).
10. Wire Iceberg credential vending end-to-end (at least one cloud) and audit vend/deny decisions.
11. Establish an engine compatibility bar: Spark/Flink/Trino configs + an interop test matrix/conformance suite that runs through the API-layer auth model.

---

## 4) Prioritized Next-PR List (Small PR Slices)

Each PR slice is intended to be reviewable and test-backed.

### P0 PRs (Before External Deployment)

| PR | Goal | Scope | Tests/Verification |
|----|------|-------|-------------------|
| PR-0 | Runtime posture contract | ✅ Implemented: `Posture` enum + env mapping (`ARCO_ENVIRONMENT` + `ARCO_API_PUBLIC`) + startup guardrail. See **PR-0 Design** below. | Unit tests for posture mapping + guardrail (`crates/arco-api/src/config.rs:532`, `crates/arco-api/src/server.rs:736`) |

#### PR-0 Design: Posture Enum

**Enum definition:**

```rust
pub enum Posture {
    Dev,      // Local development, CI. Debug allowed, /metrics open.
    Private,  // Internal deployment (api_public=false). Debug forbidden, /metrics open.
    Public,   // External deployment (api_public=true). Debug forbidden, /metrics protected.
}
```

**Mapping from environment:**

| `ARCO_ENVIRONMENT` | `api_public` (infra) | Resulting Posture | Debug | `/metrics` | JWT iss/aud |
|--------------------|----------------------|-------------------|-------|------------|-------------|
| `dev`              | `false`              | `Dev`             | ✅ allowed | open | optional |
| `dev`              | `true`               | `Public`          | ❌ forbidden | protected | required |
| `staging`          | `false`              | `Private`         | ❌ forbidden | open | required |
| `staging`          | `true`               | `Public`          | ❌ forbidden | protected | required |
| `prod`             | `false`              | `Private`         | ❌ forbidden | open | required |
| `prod`             | `true`               | `Public`          | ❌ forbidden | protected | required |

**Invariants enforced at startup (`server.rs::validate_config`):**

- `Posture::Dev`: allows `debug=true` (no posture-specific restrictions beyond existing config checks)
- `Posture::Private` / `Posture::Public`: `debug=false` required (`crates/arco-api/src/server.rs:573`) and JWT secret/public key required when `debug=false` (`crates/arco-api/src/server.rs:599`)

**Not yet enforced by PR-0 (tracked by PR-2 / PR-3a):**
- `Posture::Public`: require `iss`/`aud` and protect `/metrics`.

**Note:** Implemented option (a): `ARCO_API_PUBLIC` is now required and wired via Terraform (`crates/arco-api/src/config.rs:343`, `infra/terraform/cloud_run.tf:125`).
| PR-1 | Debug guardrail | ✅ Implemented: header-based scoping requires `debug && posture.is_dev()` for API + task callbacks. | Unit tests for RequestContext + task callbacks (`crates/arco-api/src/context.rs:292`, `crates/arco-api/src/context.rs:315`, `crates/arco-api/src/routes/tasks.rs:1234`, `crates/arco-api/src/routes/tasks.rs:1273`) |
| PR-2 | JWT claim policy hardening | Require `iss`/`aud` when `debug=false` in production posture; document allowed algs (HS256/RS256). | Unit tests for empty env vars treated as unset (`crates/arco-api/src/config.rs:302`); integration test for reject |
| PR-3a | `/metrics` access control | Define and implement a `/metrics` access strategy. See **PR-3a Design** below. | See PR-3a DoD below |

#### PR-3a Design: `/metrics` Access Strategy

**Options (pick ONE before merge):**

1. **Separate Cloud Run service**: Deploy a metrics-only service on internal ingress; scrapers hit that instead of API.
2. **Internal-only ingress for API**: Accept that public API mode is incompatible with Prometheus scraping; use push-based metrics (OTLP) instead.
3. **Auth gate on `/metrics`**: Add bearer-token or IP-allowlist protection to `/metrics`; update OTel Collector config.

**PR-3a Definition of Done:**

- [ ] Strategy decision documented in `docs/runbooks/metrics-access.md` or ADR
- [ ] Migration path for existing scrapers defined (OTel Collector config changes)
- [ ] Integration test: `Posture::Public` returns 403/404 for `/metrics` (or redirects to internal endpoint)
- [ ] If strategy requires infra changes, Terraform updated or follow-up PR linked
| PR-3b | Metrics label policy | Remove attacker-controlled label values (MatchedPath fallback) and reduce/remove tenant/workspace labels (hashing alone is insufficient). | Unit tests verifying stable/low-cardinality labels (`crates/arco-api/src/metrics.rs:97`) |
| PR-4 | Enforce bucket public access prevention | Terraform: set `public_access_prevention`; document rationale. | `terraform validate` + `terraform fmt`; captured evidence |
| PR-5 | Verify IAM list semantics (anti-entropy) | Add a runbook/script proving prefix-scoped listing behavior with SA impersonation. | Captured evidence artifact + optional nightly run |
| PR-6a | Audit foundation | Define audit event schema + sink semantics (best-effort vs must-write) + redaction/property tests. | Unit tests for schema stability + redaction; DoS controls documented |
| PR-6b | Audit instrumentation | Emit audit events for auth/url-mint/credential vend decisions + Iceberg commit. | Integration tests that assert events emitted and never include secrets |

### P0 (Iceberg Engine Parity Track)

| PR | Goal | Scope | Tests/Verification |
|----|------|-------|-------------------|
| PR-11 | Iceberg REST parameter correctness | Parse `snapshots` and `planId` (or remove from OpenAPI until implemented) and keep `/v1/config` advertisement truthful. | Route tests + OpenAPI compliance expansions |
| PR-7 | Iceberg REST write endpoints (namespaces) | Add namespace create/delete/properties endpoints; keep `/v1/config` capability hiding accurate. | Route tests; engine-oriented integration tests through API JWT auth |
| PR-8 | Iceberg REST write endpoints (tables) | Add table create/drop/register endpoints; implement `purgeRequested` semantics. | Route tests; integration tests exercising storage layout invariants |
| PR-9 | Iceberg REST rename + metrics correctness | Implement `POST /v1/{prefix}/tables/rename` and `POST .../metrics` (method/path correctness). | OpenAPI compliance + request parsing tests |
| PR-10 | Iceberg credential vending (GCS first) | Implement and wire a safe `CredentialProvider` for GCS. See **PR-10 Design** below. Depends on PR-6a. | Unit tests for scope/TTL; integration test hitting `/credentials` |

#### PR-10 Design: GCS Credential Vending

**Credential flow:**

- Use GCS **signed URLs** (not service account impersonation) for initial implementation:
  - Simpler: no IAM policy changes per table
  - Scoped: each signed URL is path-specific
  - Auditable: vend decision logged before URL generation

**Scope-down mechanism:**

- Vended credentials cover only paths under `table.location` prefix
- For read-only access: sign GET requests only
- For read-write access (commit path): sign GET + PUT for metadata paths

**TTL policy:**

| Parameter | Default | Max | Notes |
|-----------|---------|-----|-------|
| `credential_ttl` | 15 min | 1 hour | Matches browser signed URL policy |

**Audit events (requires PR-6a):**

- `CRED_VEND_ALLOW`: `table_id`, `delegation_mode`, `ttl`, `path_prefix`
- `CRED_VEND_DENY`: `table_id`, `delegation_mode`, `reason`

**Future (not PR-10):**

- Service account impersonation via Workload Identity Federation (WIF) for longer-lived credentials
- AWS S3 credential vending (STS AssumeRole)
| PR-12 | Engine compatibility matrix (GA bar) | Add “known-good” Spark/Flink/Trino configs + a minimal automated conformance suite (CI/nightly split as needed). | Documented matrix + automated smoke tests |

### P1 PRs (Next Quarter)

| PR | Goal | Scope | Tests/Verification |
|----|------|-------|-------------------|
| PR-13 | **JWKS support (explicit backlog item)** | Add `jwks_url` + caching + rotation support; replace or delete dead `JwtVerifier`. | Unit tests for JWKS cache + kid selection; integration test for rotated keys |
| PR-14 | Catalog actor attribution | Use authenticated principal identity (user claim) for catalog mutation attribution; align with audit events. | Unit tests + integration tests showing actor attribution |
| PR-15 | ID spec alignment | Resolve underscore/format inconsistencies between API/storage validation and core `TenantId`/ADR wire formats. | Unit tests; ADR/doc update |

---

## 5) CI Plan (PR-Gated vs Nightly)

**PR-gated (fast, deterministic):**

- `cargo test --workspace`
- `cargo clippy --workspace -- -D warnings`
- `cargo fmt --check`
- Terraform: `terraform fmt -check` + `terraform validate` on `infra/terraform/`

**Nightly / environment-dependent:**

- GCP integration checks for IAM condition semantics (prefix-scoped list, no-list invariants)
- Security regression suite:
  - Debug-mode guardrail
  - `/metrics` exposure rules + label policy
  - Signed URL minting allowlist cannot mint non-allowlisted paths
- Engine interop smoke suite (Spark/Flink/Trino “MUST” matrix rows)

---

## 6) In-Repo GA Bar (Interop + Parity)

Parity docs are currently external to the repo. To make progress auditable from *this* repo alone, treat the following as the in-repo GA bar:

- A checked-in engine compatibility matrix (Spark/Flink/Trino) with exact versions + config snippets.
- Automated tests that exercise Iceberg REST **through the API-layer auth model** (JWT, debug=false), not just the Iceberg router + custom headers (`crates/arco-integration-tests/tests/iceberg_rest_catalog.rs:132`).
- OpenAPI compliance tests that cover at least: content-types, required headers (ETag), and behavior for endpoints in the engine matrix (not just param names / numeric status codes) (`crates/arco-iceberg/tests/openapi_compliance.rs:107`).

---

## 7) Explicit Backlog Tracking: JWKS

**JWKS is explicitly a backlog item.** It should not be treated as “present” just because `crates/arco-api/src/auth.rs` exists; it is currently not compiled or wired (`crates/arco-api/src/lib.rs:52`).
