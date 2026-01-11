# Plan Deltas After Security/Ops + Parity Review

**Date:** 2026-01-02 (updated 2026-01-08)  
**Commit:** 966b8eecce6348821c258e38553ed945e314cfde (updated with P0 gap closures)  
**Inputs:**
- `docs/catalog-metastore/evidence/security-ops-evidence-pack.md`
- Parity/PRD working docs (local, not in repo):
  - `Iceberg-Native Feature Parity, Architecture, and Delivery Plan.md`
  - `arco-catalog-gap-analysis.md`
  - `arco-catalog-feature-parity.md`
  - `arco_catalog_prd_tech_spec_parity_checklist.md`

This document captures the delta between “current state” and parity/security requirements after producing the evidence pack and cross-checking the parity/PRD documents.

---

## Cross-Document Consistency Checklist

When updating any claim status or endpoint/parameter behavior, ensure consistency across:

| Document | What to update |
|----------|----------------|
| `2026-01-06-traceability-matrix.md` | Claim status (Implemented/Partial/Missing), code references |
| `security-ops-evidence-pack.md` | Section narrative, "What we verified" bullets, gap notes |
| `plan-deltas.md` (this doc) | Parity table status, gap summary if applicable |

**Before marking a claim as "Implemented":**
1. Verify code reference still matches (file:line)
2. Verify no contradictory statements exist in other evidence docs
3. Update all three documents atomically in the same PR

---

## 1) What Changed After Review

Key findings that directly affect the delivery plan:

- The architecture treats **multi-tenancy**, **secrets never in events/logs**, and **audit trail covers all mutations** as completeness criteria (`docs/plans/2025-01-12-arco-unified-platform-design.md:4130`, `docs/plans/2025-01-12-arco-unified-platform-design.md:4131`, `docs/plans/2025-01-12-arco-unified-platform-design.md:4132`).
- Runtime posture contract implemented: `Posture` is derived from required `ARCO_ENVIRONMENT` + `ARCO_API_PUBLIC`, and Terraform wires `ARCO_API_PUBLIC` (`crates/arco-api/src/config.rs:13`, `crates/arco-api/src/config.rs:343`, `infra/terraform/cloud_run.tf:119`, `infra/terraform/cloud_run.tf:125`).
- Debug-mode (`ARCO_DEBUG=true`) is a tenant isolation bypass for API requests and task callbacks, but header-based scoping now requires `debug && posture.is_dev()` and startup rejects debug outside dev (`crates/arco-api/src/context.rs:80`, `crates/arco-api/src/routes/tasks.rs:602`, `crates/arco-api/src/server.rs:578`).
- JWT verification supports HS256/RS256, and issuer/audience are required when `debug=false` and posture != dev; missing claims are rejected (`crates/arco-api/src/server.rs:615`, `crates/arco-api/src/context.rs:125`, `crates/arco-api/src/context.rs:246`, `crates/arco-api/tests/api_integration.rs:1394`).
- API `/metrics` returns 404 in `Posture::Public`; compactor `/metrics` supports an optional shared-secret gate when `ARCO_METRICS_SECRET` is non-empty (trimmed) (`crates/arco-api/src/server.rs:374`, `crates/arco-api/src/server.rs:384`, `crates/arco-compactor/src/main.rs:895-1003`).
- Metrics label cardinality is bounded by route templates:
  - `endpoint` uses `MatchedPath` with `UNMATCHED_ENDPOINT` fallback (no raw path) (`crates/arco-api/src/metrics.rs:90`).
  - API + Iceberg request metrics label only `endpoint/method/status_class` (no tenant/workspace labels) (`crates/arco-api/src/metrics.rs:117`, `crates/arco-iceberg/src/metrics.rs:179`).
- Storage layer scoping is strong (`ScopedStorage`) and defends against traversal and cross-scope reads (`crates/arco-core/src/scoped_storage.rs:115`, `crates/arco-core/src/scoped_storage.rs:821`).
- Signed URL minting is manifest-allowlisted, tenant-scoped, and TTL-bounded (`crates/arco-api/src/routes/browser.rs:165`, `crates/arco-api/src/routes/browser.rs:127`, `crates/arco-catalog/src/reader.rs:494`, `crates/arco-catalog/src/reader.rs:601`), but lacks the *design-intended audit record at mint time* (`crates/arco-api/src/routes/browser.rs:143`, `docs/adr/adr-019-existence-privacy.md:101`).
- Catalog mutations are attributed as `api:{tenant}` rather than the authenticated principal identity (`crates/arco-api/src/routes/namespaces.rs:115`, `crates/arco-api/src/routes/tables.rs:170`), reducing forensic/audit value even if durable audit logs are added.
- Iceberg REST is mounted and protected by outer middleware (`crates/arco-api/src/server.rs:399`, `crates/arco-api/src/server.rs:402`), but the current Iceberg router only implements a Phase A subset + `commit_table` (`crates/arco-iceberg/src/router.rs:37`, `crates/arco-iceberg/src/types/config.rs:8`).
- Iceberg credential vending is **not wired end-to-end** (provider not attached) even though the handler requires one (`crates/arco-iceberg/src/state.rs:58`, `crates/arco-api/src/server.rs:402`, `crates/arco-iceberg/src/routes/tables.rs:578`).
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
| Iceberg REST: table create/drop/register | P0 | ✅ | Routed when `allow_table_crud` enabled (`crates/arco-iceberg/src/routes/tables.rs:44-58`) |
| Iceberg REST: table rename | P0 | ✅ | Implemented at `crates/arco-iceberg/src/routes/catalog.rs:59`; within-namespace only; advertised when `allow_table_crud` |
| Iceberg REST: metrics reporting | P0 | ✅ | Implemented at `crates/arco-iceberg/src/routes/tables.rs:999`; always advertised |
| Iceberg REST: transactions/commit | P1 | ⚠️ | Single-table bridge only (`crates/arco-iceberg/src/routes/catalog.rs:132`); NOT advertised; multi-table atomic remains backlog |
| Iceberg REST: spec-declared params are actually parsed | P0 | ✅ | `snapshots` query param now parsed via `LoadTableQuery` (`crates/arco-iceberg/src/routes/tables.rs:133`, `crates/arco-iceberg/src/types/table.rs:63-87`). Filtering implemented: `refs` returns only snapshots referenced by branches/tags (`crates/arco-iceberg/src/routes/tables.rs:241-244`). `planId` documented in OpenAPI (`crates/arco-iceberg/src/routes/tables.rs:900`) and parsed/logged via `CredentialsQuery` (`crates/arco-iceberg/src/routes/tables.rs:919-924`); no behavioral effect yet (scan planning not implemented). |
| Iceberg REST: credential vending endpoint is wired and audited | P0 | ✅ | Endpoint wired with GCS provider (`crates/arco-api/src/server.rs:630-637`); audit events emitted via `emit_cred_vend_allow`/`emit_cred_vend_deny` (`crates/arco-iceberg/src/audit.rs:67,96`); handler at `crates/arco-iceberg/src/routes/tables.rs:940-960` |
| Browser read path: signed URL minting allowlisted + tenant-scoped + TTL bounded | P0 | ✅ | `crates/arco-api/src/routes/browser.rs:119`, `crates/arco-api/src/routes/browser.rs:165`, `crates/arco-catalog/src/reader.rs:494`, `crates/arco-catalog/src/reader.rs:601` |
| API auth: debug headers for dev posture vs Bearer JWT for non-dev | P0 | ⚠️ | `crates/arco-api/src/context.rs:80`, `crates/arco-api/src/context.rs:94`, `crates/arco-api/src/context.rs:297` |
| Security audit trail for decisions + mutations | P0 | ✅ | Auth allow/deny + URL mint allow/deny + credential vending + Iceberg commit audit all implemented. Auth: `crates/arco-api/src/audit.rs`, `crates/arco-api/src/context.rs:314-319`. URL mint: `crates/arco-api/src/routes/browser.rs:140,177,198`. Credential vending: `crates/arco-iceberg/src/audit.rs:67,96`. Iceberg commit: `crates/arco-iceberg/src/audit.rs:129,165`. Tier-1 commit records provide tamper-evident mutation history but lack principal attribution (`crates/arco-catalog/src/manifest.rs:738`) |
| Engine interop: Spark/Flink/Trino “MUST PASS” matrix + known-good configs | P0 | ❌ | Not present in repo; the existing Iceberg test uses custom headers (`crates/arco-integration-tests/tests/iceberg_rest_catalog.rs:132`) |

---

## 3) Updated P0 Blockers

### 3.1 Security/Ops (P0)

1. ✅ Implemented explicit runtime posture contract via required `ARCO_ENVIRONMENT` + `ARCO_API_PUBLIC` (`crates/arco-api/src/config.rs:343`, `infra/terraform/cloud_run.tf:119`, `infra/terraform/cloud_run.tf:125`).
2. ✅ Prevent `ARCO_DEBUG=true` outside dev posture at startup, and require `debug && posture.is_dev()` for header-based scoping in API + task callbacks (`crates/arco-api/src/server.rs:578`, `crates/arco-api/src/context.rs:80`, `crates/arco-api/src/routes/tasks.rs:602`). Tests cover dev accept + non-dev reject (`crates/arco-api/src/context.rs:331`, `crates/arco-api/src/context.rs:355`, `crates/arco-api/src/routes/tasks.rs:1234`, `crates/arco-api/src/routes/tasks.rs:1273`).
3. ✅ Require JWT `iss` + `aud` policy in production posture; fail closed when unset/empty (`crates/arco-api/src/server.rs:615`, `crates/arco-api/src/context.rs:125`, `crates/arco-api/tests/api_integration.rs:1394`).
4. ✅ Block API `/metrics` in `Posture::Public`; metrics label policy implemented; compactor `/metrics` supports optional shared-secret gate via `ARCO_METRICS_SECRET` (empty/whitespace disables) (`crates/arco-api/src/server.rs:374`, `crates/arco-api/src/server.rs:384`, `crates/arco-compactor/src/main.rs:895-1003`).
5. ✅ Audit foundation + baseline instrumentation implemented: `AuditEvent` schema + `AuditAction` enum + secret redaction + sink config (`crates/arco-core/src/audit.rs`). Auth allow/deny + URL mint allow/deny instrumented (`crates/arco-api/src/audit.rs`, `crates/arco-api/src/context.rs:314-319`, `crates/arco-api/src/routes/browser.rs:140,177,198`). Credential vending + Iceberg commit deferred to P1.
6. ✅ Audit failure semantics: best-effort default with `AuditFailureMode` enum; DoS controls via rate limits + sampling in `AuditSinkConfig` (`crates/arco-core/src/audit.rs:442`, `crates/arco-core/src/audit.rs:449`).
7. ✅ Bucket public access prevention enforced in Terraform (`infra/terraform/main.tf:63`). IAM list semantics verification pending (P0-6).

### 3.2 Iceberg Parity / Engine Readiness (P0)

8. Fix “spec says it exists but handler can’t read it” issues (query params, capability advertisement truthfulness) before adding new endpoints.
9. ~~Implement missing Iceberg REST baseline endpoints~~: ✅ Table rename implemented (`crates/arco-iceberg/src/routes/catalog.rs:59`); ✅ metrics reporting implemented (`crates/arco-iceberg/src/routes/tables.rs:999`); ⚠️ transactions/commit single-table bridge only (`crates/arco-iceberg/src/routes/catalog.rs:132`, not advertised). Remaining: namespaces CRUD + properties, `purgeRequested` semantics, multi-table atomic transactions.
10. ✅ Wire Iceberg credential vending end-to-end (GCS) and audit vend/deny decisions — COMPLETE (`crates/arco-api/src/server.rs:630-637`, `crates/arco-iceberg/src/audit.rs:67,96`).
11. Establish an engine compatibility bar: Spark/Flink/Trino configs + an interop test matrix/conformance suite that runs through the API-layer auth model.

---

## 4) Prioritized Next-PR List (Small PR Slices)

Each PR slice is intended to be reviewable and test-backed.

### P0 PRs (Before External Deployment)

| PR | Goal | Scope | Tests/Verification |
|----|------|-------|-------------------|
| PR-0 | Runtime posture contract | ✅ Implemented: `Posture` enum + env mapping (`ARCO_ENVIRONMENT` + `ARCO_API_PUBLIC`) + startup guardrail. See **PR-0 Design** below. | Unit tests for posture mapping + env validation + guardrail (`crates/arco-api/src/config.rs:545`, `crates/arco-api/src/config.rs:556`, `crates/arco-api/src/server.rs:784`) |

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
- `Posture::Private` / `Posture::Public`: `debug=false` required (`crates/arco-api/src/server.rs:578`), storage bucket required (`crates/arco-api/src/server.rs:598`), compactor URL required (`crates/arco-api/src/server.rs:604`), and JWT secret/public key required (`crates/arco-api/src/server.rs:615`)

**Now enforced by PR-3a:**
- API `/metrics` blocked in `Posture::Public` (`crates/arco-api/src/server.rs:374`, `crates/arco-api/src/server.rs:384`).

**Note:** Implemented option (a): `ARCO_API_PUBLIC` is now required and wired via Terraform (`crates/arco-api/src/config.rs:343`, `infra/terraform/cloud_run.tf:125`).
| PR-1 | Debug guardrail | ✅ Implemented: header-based scoping requires `debug && posture.is_dev()` for API + task callbacks. | Unit tests for RequestContext + task callbacks (`crates/arco-api/src/context.rs:331`, `crates/arco-api/src/context.rs:355`, `crates/arco-api/src/routes/tasks.rs:1234`, `crates/arco-api/src/routes/tasks.rs:1273`) |
| PR-2 | JWT claim policy hardening | ✅ Implemented: require `iss`/`aud` when `debug=false` and posture != dev; reject tokens missing claims. | Unit + integration tests (`crates/arco-api/src/server.rs:825`, `crates/arco-api/tests/api_integration.rs:1394`) |
| PR-3a | `/metrics` access control | ✅ Implemented API gating: public posture returns 404; compactor `/metrics` is intended to be internal-only (Cloud Run ingress + IAM), with an optional shared-secret gate via `ARCO_METRICS_SECRET` for defense-in-depth. Strategy documented in `docs/runbooks/metrics-access.md`. | Integration test (`crates/arco-api/tests/api_integration.rs:109`); gating (`crates/arco-api/src/server.rs:374`, `crates/arco-api/src/server.rs:384`) |

#### PR-3a Design: `/metrics` Access Strategy

**Decision (chosen):**

- API: `/metrics` is available in Dev/Private; in Public it returns 404.
- Compactor: `/metrics` remains primarily protected by internal-only ingress + Cloud Run IAM; optionally enable a shared-secret gate in code via non-empty `ARCO_METRICS_SECRET` (empty/whitespace disables).
- Public deployments: do not rely on scraping the public API; use an internal collector and/or an OTLP push pipeline.

See `docs/runbooks/metrics-access.md`.

**PR-3a Definition of Done:**

- [x] Strategy decision documented in `docs/runbooks/metrics-access.md`
- [x] Migration path for existing scrapers defined (OTel Collector config changes in `infra/monitoring/otel-collector.yaml`)
- [x] Integration test: `Posture::Public` returns 403/404 for `/metrics` (or redirects to internal endpoint)
- [ ] If strategy requires infra changes, Terraform updated or follow-up PR linked
| PR-3b | Metrics label policy | ✅ Implemented: `endpoint` uses `MatchedPath`/`UNMATCHED_ENDPOINT`, and request metrics avoid tenant/workspace labels. | Unit tests for bounded labels (`crates/arco-api/src/metrics.rs:245`, `crates/arco-api/src/metrics.rs:255`, `crates/arco-iceberg/src/metrics.rs:369`) |
| PR-3c | Compactor `/metrics` token gate (non-Cloud-Run) | ✅ Implemented: optional shared-secret gate for compactor `/metrics` via `ARCO_METRICS_SECRET` (trimmed; empty disables). Requests may use `X-Metrics-Secret` or `Authorization: Bearer`. | Unit tests for gate behavior (`crates/arco-compactor/src/main.rs`); runbook update (`docs/runbooks/metrics-access.md`); collector config (`infra/monitoring/otel-collector.yaml`) |
| PR-4 | Enforce bucket public access prevention | ✅ Implemented: `public_access_prevention = "enforced"` (`infra/terraform/main.tf:63`). | `terraform validate` + `terraform fmt` passed; evidence captured in security-ops-evidence-pack.md |
| PR-5 | Verify IAM list semantics (anti-entropy) | ✅ Implemented: runbook + verification script (`docs/runbooks/iam-list-semantics-verification.md`). Documents expected behaviors + evidence capture template. | Awaiting execution in sandbox env; optional nightly CI integration provided |
| PR-6a | Audit foundation | ✅ Implemented: `AuditEvent` schema + `AuditAction` enum + `AuditSinkConfig` + secret redaction + redact-and-continue strategy (`crates/arco-core/src/audit.rs`). Sink infrastructure added: `AuditSink` trait (`crates/arco-core/src/audit.rs:483`), `AuditEmitter` (`crates/arco-core/src/audit.rs:499`), `TracingAuditSink` (`crates/arco-core/src/audit.rs:593`), `TestAuditSink` (`crates/arco-core/src/audit.rs:635`). | Unit tests for schema stability + redaction + sink capture (`crates/arco-core/src/audit.rs:502-1038`) |
| PR-6b | Audit instrumentation (auth + URL mint) | ✅ Implemented: Auth allow/deny + URL mint allow/deny events instrumented. API helpers in `crates/arco-api/src/audit.rs`. Auth middleware emits at `crates/arco-api/src/context.rs:314-319`. URL minting emits at `crates/arco-api/src/routes/browser.rs:140,177,198`. Credential vending + Iceberg commit deferred to P1. | Unit tests for emit helpers (`crates/arco-api/src/audit.rs:127-154`); integration coverage via auth/URL mint paths |

### P0 (Iceberg Engine Parity Track)

| PR | Goal | Scope | Tests/Verification |
|----|------|-------|-------------------|
| PR-11 | Iceberg REST parameter correctness | ✅ Implemented: `LoadTableQuery` struct parses `snapshots` param (`crates/arco-iceberg/src/types/table.rs:63-87`). `SnapshotsFilter::Refs` filters to referenced snapshots only (`crates/arco-iceberg/src/routes/tables.rs:241-244`). `planId` documented in OpenAPI and parsed/logged (`crates/arco-iceberg/src/routes/tables.rs:900,919-924`); accepted but no behavioral effect yet (scan planning not implemented). | Unit tests for query parsing + filter default (`crates/arco-iceberg/src/types/table.rs:457-470`) |
| PR-7 | Iceberg REST write endpoints (namespaces) | Add namespace create/delete/properties endpoints; keep `/v1/config` capability hiding accurate. | Route tests; engine-oriented integration tests through API JWT auth |
| PR-8 | Iceberg REST write endpoints (tables) | Add table create/drop/register endpoints; implement `purgeRequested` semantics. | Route tests; integration tests exercising storage layout invariants |
| PR-9 | Iceberg REST rename + metrics correctness | ✅ COMPLETE: Rename at `crates/arco-iceberg/src/routes/catalog.rs:59`; metrics at `crates/arco-iceberg/src/routes/tables.rs:999`. | Tests at `crates/arco-iceberg/src/routes/catalog.rs:415`, `crates/arco-iceberg/src/routes/tables.rs:2561` |
| PR-10 | Iceberg credential vending (GCS first) | ✅ COMPLETE: GCS `CredentialProvider` wired (`crates/arco-api/src/server.rs:630-637`); audit events implemented (`crates/arco-iceberg/src/audit.rs:67,96,129,165`); TTL bounded (`crates/arco-iceberg/src/credentials/mod.rs:33,44`). See **PR-10 Design** below. | Unit tests for scope/TTL; integration test hitting `/credentials` |

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
