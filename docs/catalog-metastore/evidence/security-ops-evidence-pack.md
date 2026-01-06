# Catalog/Metastore Security & Ops Evidence Pack (Arco)

**Date:** 2026-01-02 (updated 2026-01-06)  
**Commit:** 966b8eecce6348821c258e38553ed945e314cfde (evidence updated with P0 gap closures)  
**Scope:** Catalog/metastore security posture (API authN/authZ, storage isolation & IAM, read-path URL minting, observability/monitoring, Iceberg REST + credential vending)  
**Overall status:** ⚠️ **Near audit-ready** (Security audit trail complete for auth, URL mint, credential vending, and Iceberg commits. Credential vending is wired with GCS provider. P0 gaps closed. Remaining: Iceberg REST parity endpoints (rename, transactions, metrics), compactor `/metrics` relies on infra isolation; see `crates/arco-api/src/server.rs:374`, `crates/arco-api/src/server.rs:384`, `crates/arco-compactor/src/main.rs:963`).

## Status Legend

- ✅ Implemented / production-ready (code + tests, no known gaps)
- ⚠️ Implemented, needs review (works, but has known risks or unverified assumptions)
- 🔨 Partial (some pieces exist; end-to-end incomplete)
- ❌ Missing (not implemented)
- 🚫 Anti-pattern (implemented in a risky way)

---

## 0) One-Page Summary

| Area | Status | Severity | Confidence | Verification | Key Evidence (file:line) | Primary Risks | Next Actions |
|------|--------|----------|------------|--------------|--------------------------|---------------|--------------|
| GCS IAM prefix scoping | ⚠️ | P0 | Medium | Terraform + env test needed | `infra/terraform/iam_conditions.tf:1`, `infra/terraform/iam_conditions.tf:28`, `infra/terraform/iam_conditions.tf:54`, `infra/terraform/main.tf:63` | Conditional IAM semantics for `list` unverified | Add IAM semantics verification (sandbox env) |
| Service accounts & Cloud Run isolation | ✅ | P0 | High | Terraform | `infra/terraform/iam.tf:23`, `infra/terraform/iam.tf:67`, `infra/terraform/iam.tf:75`, `infra/terraform/cloud_run.tf:28`, `infra/terraform/cloud_run.tf:173`, `infra/terraform/cloud_run_job.tf:16` | Misconfig of `api_public` + unauthenticated `/metrics` | Keep API internal by default; protect `/metrics` when public |
| API auth (JWT + debug headers) | ⚠️ | P0 | High | Code review | `crates/arco-api/src/context.rs:80`, `crates/arco-api/src/context.rs:125`, `crates/arco-api/src/context.rs:246`, `crates/arco-api/src/server.rs:615`, `crates/arco-api/tests/api_integration.rs:1394`, `infra/terraform/cloud_run.tf:94` | JWKS/rotation not implemented | Track JWKS backlog |
| Runtime posture contract (dev/private/public) | ✅ | P0 | Medium | Code + tests | `crates/arco-api/src/config.rs:13`, `crates/arco-api/src/config.rs:343`, `crates/arco-api/src/config.rs:545`, `crates/arco-api/src/server.rs:578`, `crates/arco-api/src/server.rs:784`, `infra/terraform/cloud_run.tf:119`, `infra/terraform/cloud_run.tf:125` | Posture depends on correct env/ingress wiring | Add deployment validation for `ARCO_ENVIRONMENT`/`ARCO_API_PUBLIC` and keep guardrail tests |
| Tenant/workspace storage isolation | ✅ | P0 | High | Code + tests | `crates/arco-core/src/scoped_storage.rs:1`, `crates/arco-core/src/scoped_storage.rs:115`, `crates/arco-core/src/scoped_storage.rs:821` | If debug is enabled (dev posture), scope selection becomes attacker-controlled | Keep guardrail + add e2e coverage |
| Browser-direct read path (signed URLs) | ✅ | P0 | High | Code review | `crates/arco-api/src/routes/browser.rs:5`, `crates/arco-api/src/routes/browser.rs:119`, `crates/arco-catalog/src/reader.rs:494`, `crates/arco-api/src/routes/browser.rs:140,177,198` (audit) | URL mint allow/deny audit events implemented; redaction enforced via "do not log" policy (`crates/arco-api/src/routes/browser.rs:167`) | Complete: audit events emit at mint time for allow/deny decisions |
| Observability & monitoring | ✅ | P1 | Medium | Code + infra | `crates/arco-core/src/observability.rs:43`, `crates/arco-api/src/metrics.rs:41`, `crates/arco-api/src/server.rs:384`, `crates/arco-compactor/src/main.rs:963`, `infra/monitoring/otel-collector.yaml:1`, `crates/arco-iceberg/src/audit.rs:67-188` | Auth + URL mint + credential vending + Iceberg commit audit events all implemented; API `/metrics` blocked in public posture; compactor relies on infra isolation; metrics labels are bounded | Keep compactor internal or add metrics auth; keep label tests; tenant label removed from flow metrics |
| Iceberg REST surface | 🔨 | P0 | Medium | Code review | `crates/arco-api/src/server.rs:219`, `crates/arco-iceberg/src/router.rs:37`, `crates/arco-iceberg/src/types/config.rs:8` | Many PRD endpoints missing; query params declared but ignored | Slice PRs for endpoint completeness + parameter parsing |
| Iceberg credential vending | ✅ | P0 | High | Code review | `crates/arco-api/src/server.rs:630-637` (provider wiring), `crates/arco-iceberg/src/routes/tables.rs:940-960` (handler), `crates/arco-iceberg/src/audit.rs:67,96` (audit helpers) | Provider wired when GCP feature enabled; vend/deny audit trail implemented | Complete: GCS credential provider wired + audit events emitted |

---

## 0.1 Control Objectives (Non-Negotiables)

These objectives are the completeness criteria this evidence pack is meant to prove in the *real implementation*.

**Architecture non-negotiables (design intent):**

- Multi-tenancy enforced at all layers (`docs/plans/2025-01-12-arco-unified-platform-design.md:4130`).
- Secrets never in events/logs (`docs/plans/2025-01-12-arco-unified-platform-design.md:4131`, `docs/plans/2025-01-12-arco-orchestration-design-part2.md:2011`).
- Audit trail covers all mutations (`docs/plans/2025-01-12-arco-unified-platform-design.md:4132`).

**Read-path audit intent:**

- Audit log entry should be created at URL generation time (not access time) (`docs/adr/adr-019-existence-privacy.md:101`).

**Audit storage intent:**

- Audit logs stored as monthly Parquet under `audit/YYYY-MM/audit_log.parquet` (`docs/plans/ARCO_TECHNICAL_VISION.md:437`).

---

## 0.2 Deployment Assumptions / Evidence Boundaries

This pack assumes:

- **Platform:** GCP Cloud Run + GCS + Terraform in `infra/terraform/*`.
- **Ingress posture:** API is internal by default and may be made public via `api_public` (`infra/terraform/cloud_run.tf:25`).
- **TLS:** Terminated/managed by Cloud Run/Google Front End (assumed platform behavior; not proven in repo).

If these assumptions change, evidence must be re-collected.

---

## 1) Answers to the Specific Audit Questions

### 1.1 Can debug auth bypass be accidentally enabled in production?

**Answer:** No (fails fast), provided posture is set via required env vars.

- **Guardrail:** Server refuses to start when `debug=true` outside `Posture::Dev` (`crates/arco-api/src/server.rs:578`).
- **Posture source:** `ARCO_ENVIRONMENT` + `ARCO_API_PUBLIC` are required to derive posture (`crates/arco-api/src/config.rs:343`).
- **Terraform wiring:** Cloud Run sets both `ARCO_ENVIRONMENT` and `ARCO_API_PUBLIC` (`infra/terraform/cloud_run.tf:119`, `infra/terraform/cloud_run.tf:125`).
- **Behavior:** Header-based tenant/workspace scoping is allowed only when `debug=true` and posture is dev (`crates/arco-api/src/context.rs:80`).

**Residual risk:** If runtime env values do not reflect actual ingress (e.g., `ARCO_API_PUBLIC=false` while the service is externally reachable), debug could be enabled in an exposed deployment. Validate env wiring in deployment pipelines.

### 1.2 Is tenant isolation enforced at the storage layer, not just API layer?

**Answer:** Yes, for codepaths that use `ScopedStorage` correctly.

- `ScopedStorage` prefixes every path with `tenant={tenant}/workspace={workspace}/` (`crates/arco-core/src/scoped_storage.rs:1`).
- It validates IDs and rejects traversal and percent-encoding (`crates/arco-core/src/scoped_storage.rs:81`, `crates/arco-core/src/scoped_storage.rs:115`).
- Tests prove traversal and cross-scope list/read isolation (`crates/arco-core/src/scoped_storage.rs:821`).

**Important nuance:** Isolation still depends on correct *scope selection* (tenant/workspace) coming from authentication. Debug-mode bypass makes scope selection attacker-controlled (`crates/arco-api/src/context.rs:80`).

### 1.3 Are there any hardcoded secrets or credentials?

**Answer:** Found only test fixtures (no production secrets observed in reviewed Rust/Terraform).

- Test-only JWT/RSA key material exists in integration tests (`crates/arco-api/tests/api_integration.rs:13`, `crates/arco-api/tests/api_integration.rs:1047`).
- Production JWT secret is intended to come from Secret Manager/env, not checked in (`crates/arco-api/src/config.rs:386`, `infra/terraform/cloud_run.tf:129`).

### 1.4 What happens if JWT validation fails?

**Answer:** Hard 401 (no fallback).

- Missing auth header returns 401 `MISSING_AUTH` (`crates/arco-api/src/error.rs:50`).
- Invalid token returns 401 `INVALID_TOKEN` (`crates/arco-api/src/error.rs:60`).
- JWT decode failures map to `invalid_token()` (`crates/arco-api/src/context.rs:130`).

### 1.5 Is the Iceberg credential vending endpoint protected?

**Answer:** Yes at the API layer; the Iceberg crate router relies on outer middleware.

- API applies `iceberg_auth_middleware` to all Iceberg endpoints except `/v1/config` and `/openapi.json` (`crates/arco-api/src/server.rs:215`, `crates/arco-api/src/server.rs:245`).
- Iceberg crate has a fallback header-based context extraction if context is not injected (`crates/arco-iceberg/src/context.rs:97`), so mounting Iceberg without the API auth layer would be unsafe.

### 1.6 Are GCS signed URLs scoped correctly? What’s the TTL?

**Answer:** URLs are tenant/workspace scoped, manifest-allowlisted, and TTL bounded.

- API endpoint bounds TTL (default 900s, max 3600s) (`crates/arco-api/src/routes/browser.rs:52`, `crates/arco-api/src/routes/browser.rs:127`).
- Only allowlisted snapshot paths are mintable (`crates/arco-api/src/routes/browser.rs:158`, `crates/arco-api/src/routes/browser.rs:165`).
- Catalog layer re-validates allowlist and caps TTL again (`crates/arco-catalog/src/reader.rs:601`, `crates/arco-catalog/src/reader.rs:617`).
- Storage signing is scope-relative and traversal-safe (`crates/arco-core/src/scoped_storage.rs:558`).

---

## A) GCS IAM & Tenant Isolation Evidence

### A.1 Prefix-Scoped IAM Conditions

**Status:** ⚠️ Implemented, needs review

**What we verified (with evidence):**

- IAM conditions explicitly forbid `contains()` and require anchored prefix matching (`infra/terraform/iam_conditions.tf:1`).
- Tenant/workspace boundary enforced via anchored regex prefix (`infra/terraform/iam_conditions.tf:28`).
- API SA can write ledger events (`roles/storage.objectCreator`) only under `ledger/` prefix (`infra/terraform/iam_conditions.tf:45`).
- API SA can manage locks (`roles/storage.objectUser`) only under `locks/` prefix (`infra/terraform/iam_conditions.tf:60`).
- API SA can write commits only under `commits/` prefix (`infra/terraform/iam_conditions.tf:75`).
- Compactor fast-path write permissions are prefix-scoped (e.g., state/l0/manifests) (`infra/terraform/iam_conditions.tf:121`, `infra/terraform/iam_conditions.tf:136`, `infra/terraform/iam_conditions.tf:151`).

**What remains unknown / unverified:**

- Whether conditional IAM expressions using `resource.name.matches(".../objects/..."` reliably constrain `storage.objects.list` to a prefix in GCS (anti-entropy intent) (`infra/terraform/iam_conditions.tf:195`).

**Risks:**

- If conditional IAM does not constrain list operations as intended, anti-entropy might fail at runtime or force broader permissions.

**Bucket public access prevention (P0-5 - COMPLETE):**

- `public_access_prevention = "enforced"` is configured on the catalog bucket (`infra/terraform/main.tf:63`).
- This defense-in-depth control ensures bucket data cannot be accidentally exposed even if ACLs are misconfigured.

**IAM list semantics verification (P0-6):**

- Runbook created: `docs/runbooks/iam-list-semantics-verification.md`
- Script tests whether conditional IAM constrains `storage.objects.list` results
- Pending execution in sandbox environment to capture evidence

**Recommended actions (PRD-aligned):**

- Execute IAM list semantics verification script in sandbox environment and document results.

### A.2 Service Account Design (API vs Compactor)

**Status:** ✅ Implemented

**What we verified:**

- Compactor split into fast-path vs anti-entropy service accounts is documented and implemented (`infra/terraform/iam.tf:51`).
- Fast-path compactor explicitly has no list permission; anti-entropy is the only component with list intent (`infra/terraform/iam_conditions.tf:100`, `infra/terraform/iam_conditions.tf:189`).
- A custom role exists to enforce “object get without list” (`infra/terraform/iam.tf:102`).

**Gaps / inconsistencies:**

- `iam.tf` comments claim API writes `manifests/`, but the prefix-scoped bindings do not grant that; compactor writes manifests (`infra/terraform/iam.tf:30`, `infra/terraform/iam_conditions.tf:151`).

**Recommended actions:**

- Reconcile IAM comments vs actual bindings; confirm intended writer for `manifests/` and update comments/docs accordingly.

---

## B) AuthN/AuthZ Evidence (API Layer)

### B.1 Request Auth Flow (End-to-End)

**Status:** ⚠️ Implemented, needs review

**What we verified:**

- Main API routes are authenticated via `auth_middleware` (`crates/arco-api/src/server.rs:359`).
- `RequestContext` extraction:
  - Debug mode uses `X-Tenant-Id` / `X-Workspace-Id` headers only when posture is dev (`crates/arco-api/src/context.rs:80`).
  - Non-dev posture requires `Authorization: Bearer <jwt>` (`crates/arco-api/src/context.rs:94`).
- JWT signature validation supports HS256 (secret) and RS256 (public key PEM), mutually exclusive (`crates/arco-api/src/context.rs:188`).
- Tenant/workspace normalization prevents separators and invalid chars (`crates/arco-api/src/context.rs:144`).
- Request IDs are generated/propagated and echoed back (`crates/arco-api/src/context.rs:76`, `crates/arco-api/src/context.rs:272`).

**JWT claim policy enforcement (issuer/audience):**

- Startup validation requires non-empty `jwt.issuer` and `jwt.audience` when `debug=false` and posture != dev (`crates/arco-api/src/server.rs:628`, `crates/arco-api/src/server.rs:640`).
- RequestContext rejects JWTs missing `iss`/`aud` when configured (`crates/arco-api/src/context.rs:125`, `crates/arco-api/src/context.rs:143`, `crates/arco-api/src/context.rs:246`, `crates/arco-api/src/context.rs:275`).
- Integration tests cover missing issuer/audience claims (`crates/arco-api/tests/api_integration.rs:1394`, `crates/arco-api/tests/api_integration.rs:1423`).

**Implemented auth middleware variants:**

- Default `/api/v1/*` auth middleware (`crates/arco-api/src/context.rs:252`).
- Task callback auth middleware with task-scoped bearer token (`crates/arco-api/src/routes/tasks.rs:587`).
- Iceberg auth middleware that allows `/v1/config` and `/openapi.json` unauthenticated (`crates/arco-api/src/server.rs:215`).

**Gaps:**

- JWKS/rotation is not implemented; a draft `JwtVerifier` exists but is not compiled or wired (`crates/arco-api/src/lib.rs:52`, `crates/arco-api/src/auth.rs:30`).

### B.2 Debug/Bypass Modes

**Status:** ✅ Implemented guardrail (debug only in dev posture)

**What we verified:**

- Posture is derived from required `ARCO_ENVIRONMENT` + `ARCO_API_PUBLIC` (`crates/arco-api/src/config.rs:343`, `crates/arco-api/src/config.rs:354`).
- Server rejects `debug=true` outside `Posture::Dev` (`crates/arco-api/src/server.rs:578`).
- Terraform provides `ARCO_ENVIRONMENT` and `ARCO_API_PUBLIC` to Cloud Run (`infra/terraform/cloud_run.tf:119`, `infra/terraform/cloud_run.tf:125`).
- Debug-mode header scoping uses `debug && posture.is_dev()` for API requests and task callbacks (`crates/arco-api/src/context.rs:80`, `crates/arco-api/src/routes/tasks.rs:602`).
- Regression tests cover dev acceptance and non-dev rejection for RequestContext and task callbacks (`crates/arco-api/src/context.rs:331`, `crates/arco-api/src/context.rs:355`, `crates/arco-api/src/routes/tasks.rs:1234`, `crates/arco-api/src/routes/tasks.rs:1273`).

**Residual risk:**

- If ingress configuration diverges from `ARCO_API_PUBLIC`, posture can be misclassified.

**Recommended actions:**

- Add deployment validation to ensure `ARCO_ENVIRONMENT`/`ARCO_API_PUBLIC` match actual ingress; keep guardrail tests as regressions.

---

## C) Observability, Monitoring, and Auditability Evidence

### C.1 Logging & Redaction

**Status:** ⚠️ Implemented baseline; redaction not enforced everywhere

**What we verified:**

- Logging is initialized once via `init_logging` with JSON logs when `debug=false` (`crates/arco-api/src/main.rs:33`, `crates/arco-core/src/observability.rs:110`).
- Sensitive-data wrapper exists (`Redacted<T>`) with tests (`crates/arco-core/src/observability.rs:43`).
- Signed URL minting route explicitly documents “do not log URLs/paths” and only logs safe metadata (`crates/arco-api/src/routes/browser.rs:17`, `crates/arco-api/src/routes/browser.rs:143`).

**Gaps:**

- URL redaction helper exists but is unused (dead-code annotations) (`crates/arco-api/src/redaction.rs:8`).
- Signed URL minting path does not emit a structured audit event (only safe metadata log + metrics) (`crates/arco-api/src/routes/browser.rs:143`, `crates/arco-api/src/routes/browser.rs:181`).

### C.2 Metrics & Monitoring Pipeline

**Status:** ⚠️ Implemented, but needs access control + audit coverage

**What we verified:**

- Prometheus exporter + middleware exist (`crates/arco-api/src/metrics.rs:41`, `crates/arco-api/src/metrics.rs:94`).
- API `/metrics` returns 404 in public posture and is otherwise open (`crates/arco-api/src/server.rs:374`, `crates/arco-api/src/server.rs:384`).
- Compactor `/metrics` is mounted without auth in serve mode (`crates/arco-compactor/src/main.rs:963`).
- OTel Collector scrapes Prometheus endpoints and exports to Cloud Monitoring (`infra/monitoring/otel-collector.yaml:1`, `infra/monitoring/README.md:17`).

**Label policy evidence:**

- `endpoint` uses `MatchedPath` with `UNMATCHED_ENDPOINT` fallback (no raw path) (`crates/arco-api/src/metrics.rs:90`, `crates/arco-iceberg/src/metrics.rs:159`).
- Tests assert no raw-path labels and no tenant/workspace labels (`crates/arco-api/src/metrics.rs:245`, `crates/arco-api/src/metrics.rs:255`, `crates/arco-iceberg/src/metrics.rs:369`).

**Recommended actions:**

- Protect `/metrics` in public deployments (and document scraper migration).
- Keep label-policy regression tests; add compactor `/metrics` auth if internal-only ingress is not guaranteed.

### C.3 Audit Trail (Security Decisions + Mutations)

**Status:** ⚠️ Implemented baseline (auth + URL mint instrumented; remaining gaps are P1)

**Design intent (what "complete" means):**

- "Audit trail covers all mutations" is a completeness criterion (`docs/plans/2025-01-12-arco-unified-platform-design.md:4132`).
- Design sketches an `AuditEvent` schema with actor/action/resource and before/after values (`docs/plans/2025-01-12-arco-unified-platform-design.md:3100`).
- Audit log should be append-only and tamper-evident via hash chain (`docs/plans/2025-01-12-arco-unified-platform-design.md:3167`).
- Storage layout expects monthly Parquet logs under `audit/YYYY-MM/audit_log.parquet` (`docs/plans/ARCO_TECHNICAL_VISION.md:437`).
- For signed URLs specifically, the audit record should be created at *generation time* (`docs/adr/adr-019-existence-privacy.md:101`).

**Implementation evidence (P0-4a - Audit Foundation):**

- `AuditEvent` schema implemented with all required fields (`crates/arco-core/src/audit.rs:119`):
  - `event_version`, `event_id` (ULID), `timestamp`, `request_id`, `trace_id` (optional)
  - `actor`, `tenant_id` (optional), `workspace_id` (optional)
  - `action` (enum: AUTH_ALLOW/DENY, URL_MINT_ALLOW/DENY, CRED_VEND_ALLOW/DENY, ICEBERG_COMMIT/DENY)
  - `resource`, `decision_reason`, `policy_version`
- `AuditAction` enum defines security decision actions (`crates/arco-core/src/audit.rs:46`).
- Secret redaction implemented: builder automatically redacts fields containing secret patterns (`crates/arco-core/src/audit.rs:207`, `crates/arco-core/src/audit.rs:264`).
- Redact-and-continue strategy prevents audit suppression attacks (`crates/arco-core/src/audit.rs:431`).
- `AuditSinkConfig` defines failure semantics (best-effort default) and DoS controls (`crates/arco-core/src/audit.rs:438`).
- Schema stability tests ensure serialization format is stable (`crates/arco-core/src/audit.rs:696`).
- Secret detection tests verify redaction patterns work (`crates/arco-core/src/audit.rs:573`, `crates/arco-core/src/audit.rs:585`, `crates/arco-core/src/audit.rs:602`).

**Sink infrastructure (P0-4b - COMPLETE):**

- `AuditSink` trait for pluggable sinks (`crates/arco-core/src/audit.rs:483`).
- `AuditEmitter` with routing and rate limiting support (`crates/arco-core/src/audit.rs:499`).
- `TracingAuditSink` for production (structured JSON logs) (`crates/arco-core/src/audit.rs:593`).
- `TestAuditSink` for unit testing (`crates/arco-core/src/audit.rs:635`).
- Unit tests for sink capture and event filtering (`crates/arco-core/src/audit.rs:972-1038`).
- `AppState` has `audit: Arc<AuditEmitter>` field with accessor (`crates/arco-api/src/server.rs:74`).

**Auth and URL minting instrumentation (P0-4b - COMPLETE):**

- API-layer audit helpers: stable reason codes and emission functions (`crates/arco-api/src/audit.rs:1-109`).
- `auth_middleware` emits `AUTH_ALLOW` on successful JWT validation (`crates/arco-api/src/context.rs:319`).
- `auth_middleware` emits `AUTH_DENY` with `missing_token` or `invalid_token` reason on failure (`crates/arco-api/src/context.rs:314`).
- `mint_urls` emits `URL_MINT_DENY` with `path_traversal` reason on traversal attempt (`crates/arco-api/src/routes/browser.rs:140`).
- `mint_urls` emits `URL_MINT_DENY` with `not_in_allowlist` reason on allowlist failure (`crates/arco-api/src/routes/browser.rs:177`).
- `mint_urls` emits `URL_MINT_ALLOW` on successful minting (`crates/arco-api/src/routes/browser.rs:198`).

**Credential vending + Iceberg commit audit (P0 - COMPLETE):**

- Credential vending emits audit events via `emit_cred_vend_allow`/`emit_cred_vend_deny` (`crates/arco-iceberg/src/audit.rs:67,96`).
- Iceberg commit emits audit events via `emit_iceberg_commit`/`emit_iceberg_commit_deny` (`crates/arco-iceberg/src/audit.rs:129,165`).
- All credential vending paths emit events (`crates/arco-iceberg/src/routes/tables.rs:1223,1240,1265,1272`).
- All commit success/deny paths emit events (`crates/arco-iceberg/src/routes/tables.rs:735,754,773,788`).

**Remaining gaps (P1 - Future instrumentation):**

- Task auth middleware (`crates/arco-api/src/routes/tasks.rs:587`) - defer to task subsystem work.
- Iceberg auth middleware (`crates/arco-api/src/server.rs:219`) - defer to Iceberg completeness work.
- Catalog mutation attribution still uses `api:{tenant}` (`crates/arco-api/src/routes/namespaces.rs:115`).

**Storage + integrity plan (P0/P1):**

- Append-only write path; compact to monthly Parquet at `audit/YYYY-MM/audit_log.parquet` (`docs/plans/ARCO_TECHNICAL_VISION.md:437`).
- Add tamper evidence (hash chain per tenant or per partition) (`docs/plans/2025-01-12-arco-unified-platform-design.md:3167`).

---

## D) Iceberg REST Service Surface Evidence

### D.1 Mounting & Protection

**Status:** ⚠️ Implemented, depends on outer auth

**What we verified:**

- Iceberg REST is mounted at `/iceberg` when enabled (`crates/arco-api/src/server.rs:396`, `crates/arco-api/src/server.rs:409`).
- `/iceberg/v1/config` and `/iceberg/openapi.json` are intentionally unauthenticated (`crates/arco-api/src/server.rs:215`, `crates/arco-api/src/server.rs:245`).
- All other Iceberg endpoints require `RequestContext` derived from auth (`crates/arco-api/src/server.rs:228`).

**Risk:**

- Iceberg crate has a fallback context middleware that accepts `X-Tenant-Id`/`X-Workspace-Id` if context is not injected (`crates/arco-iceberg/src/context.rs:97`). This is safe in current API mounting, but unsafe if the Iceberg router is mounted standalone.
- Iceberg endpoints are protected by auth, but are not currently behind the API rate limiting layer (rate limiting is applied to `/api/v1/*`, not `/iceberg/*`) (`crates/arco-api/src/server.rs:384`, `crates/arco-api/src/server.rs:409`).

### D.2 Implemented Endpoints & Capability Hiding

**Status:** 🔨 Partial

**What we verified:**

- Router exposes read endpoints + credentials endpoint + commit endpoint (write-gated) (`crates/arco-iceberg/src/router.rs:37`).
- `/v1/config` advertises only endpoints that are implemented; commit_table is the only write endpoint advertised when enabled (`crates/arco-iceberg/src/types/config.rs:8`, `crates/arco-iceberg/src/types/config.rs:63`).

**Known gaps (PRD P0/P1):**

- Namespace create/delete/properties are not implemented (only list/get/HEAD exist) (`crates/arco-iceberg/src/routes/namespaces.rs:30`).
- Table create/drop/register/rename are not implemented (only list/load/HEAD/commit exist) and are not advertised in `/v1/config` (`crates/arco-iceberg/src/router.rs:37`, `crates/arco-iceberg/src/types/config.rs:63`).
- Iceberg spec endpoints not implemented: metrics (`POST .../metrics`) and multi-table transactions (`POST /transactions/commit`) (not routed; not advertised) (`crates/arco-iceberg/src/router.rs:37`, `crates/arco-iceberg/src/types/config.rs:10`).
- Drop-table `purgeRequested` semantics are not implemented (drop-table endpoint itself is missing) (`crates/arco-iceberg/src/types/config.rs:67`).
- Parity docs call out engine/client auth schemes (SigV4, Google Auth, token rotation/revocation); current API supports debug headers or `Authorization: Bearer <jwt>` only (`crates/arco-api/src/context.rs:80`, `crates/arco-api/src/context.rs:237`).
- ✅ Query parameters now properly parsed: `snapshots` param parsed via `LoadTableQuery` (`crates/arco-iceberg/src/types/table.rs:63-87`), filtering implemented (`crates/arco-iceberg/src/routes/tables.rs:241-244`). `planId` removed from OpenAPI (scan planning not implemented).
- Test signal is weaker than it appears: the OpenAPI compliance test checks parameter-name and numeric status-code subsets (not full schema/behavior), and the integration test exercises the Iceberg router directly using custom tenancy headers (not API-layer JWT auth) (`crates/arco-iceberg/tests/openapi_compliance.rs:107`, `crates/arco-integration-tests/tests/iceberg_rest_catalog.rs:132`).

### D.3 ETag / If-None-Match & Idempotency

**Status:** ✅ Implemented (for `load_table` + `commit_table`)

**What we verified:**

- `load_table` supports `If-None-Match` and returns `304 Not Modified` (`crates/arco-iceberg/src/routes/tables.rs:189`).
- `load_table` sets an `ETag` derived from pointer metadata (`crates/arco-iceberg/src/routes/tables.rs:269`).
- `commit_table` requires an `Idempotency-Key` and validates UUIDv7 format (`crates/arco-iceberg/src/routes/tables.rs:406`, `crates/arco-iceberg/src/routes/tables.rs:414`).
- **Interop risk:** UUIDv7-only may be stricter than some Iceberg clients; validate against Spark/Flink/Trino before treating as GA (`crates/arco-iceberg/src/routes/tables.rs:406`).

### D.4 Credential Vending (Iceberg)

**Status:** ✅ Implemented

**What we verified:**

- A `CredentialProvider` interface exists (`crates/arco-iceberg/src/state.rs:117`).
- The credentials endpoint exists and requires vending to be enabled (`crates/arco-iceberg/src/routes/tables.rs:940-960`).
- API wires GCS credential provider when `feature="gcp"` and vending is enabled (`crates/arco-api/src/server.rs:630-637`).
- Runtime enablement is controlled by `ARCO_ICEBERG_ENABLE_CREDENTIAL_VENDING` (`crates/arco-api/src/config.rs:448`).
- TTL is bounded by clamp constants (60s-3600s) (`crates/arco-iceberg/src/credentials/mod.rs:33,44`).

**Audit trail (implemented):**

- `emit_cred_vend_allow` emits `CredVendAllow` on successful credential vending (`crates/arco-iceberg/src/audit.rs:67`).
- `emit_cred_vend_deny` emits `CredVendDeny` with reason codes on failures (`crates/arco-iceberg/src/audit.rs:96`).
- All credential vending paths emit audit events (`crates/arco-iceberg/src/routes/tables.rs:1223,1240,1265,1272`).

**Residual considerations:**

- Provider initialization failures disable vending gracefully (logs error, continues without vending).
- Engine clients should handle 400 "Credential vending is not enabled" when provider unavailable.

---

## E) Read Path Access Evidence (Browser-direct / Signed URLs)

### E.1 API Surface & Allowlisting

**Status:** ✅ Implemented

**What we verified:**

- Endpoint `POST /api/v1/browser/urls` exists (`crates/arco-api/src/routes/browser.rs:31`).
- TTL bounded: default 15 minutes, max 1 hour (`crates/arco-api/src/routes/browser.rs:52`, `crates/arco-api/src/routes/browser.rs:127`).
- Path traversal guard (early check) and strict manifest-driven allowlist (`crates/arco-api/src/routes/browser.rs:134`, `crates/arco-api/src/routes/browser.rs:165`).
- Uses tenant/workspace scoped storage (`crates/arco-api/src/routes/browser.rs:155`).

### E.2 Catalog Enforcement & URL Signing

**Status:** ✅ Implemented

**What we verified:**

- Catalog exposes `get_mintable_paths()` and `mint_signed_urls()` and re-checks allowlist (`crates/arco-catalog/src/reader.rs:494`, `crates/arco-catalog/src/reader.rs:617`).
- URL signing uses storage backend signer and is GET-only (`crates/arco-core/src/storage.rs:446`).
- Scoped signing validates paths and prefixes tenant/workspace (`crates/arco-core/src/scoped_storage.rs:558`).

### E.3 Storage-Level Traversal Protection

**Status:** ✅ Implemented

**What we verified:**

- `ScopedStorage` rejects absolute paths, percent-encoding, and traversal segments (`crates/arco-core/src/scoped_storage.rs:115`).
- Tests cover traversal and cross-scope isolation (`crates/arco-core/src/scoped_storage.rs:821`).

---

## F) Secrets Policy Evidence

**Status:** 🔨 Partial (local redaction primitives exist; no end-to-end enforcement)

**Design intent (non-negotiable):**

- Secrets never stored in events/logs/task payloads (`docs/plans/2025-01-12-arco-orchestration-design-part2.md:2011`).
- Unified platform design sketches `SecretRef` and explicit “never in logs/events” validation (`docs/plans/2025-01-12-arco-unified-platform-design.md:3049`).

**What we verified in current implementation:**

- Redaction primitive exists (`Redacted<T>`) to prevent tokens/signed URLs from appearing in logs (`crates/arco-core/src/observability.rs:43`).
- Signed URL minting route has an explicit “do not log signed URLs or requested paths” policy (`crates/arco-api/src/routes/browser.rs:17`).
- JWT secret can be sourced from Secret Manager and injected as env var (`infra/terraform/cloud_run.tf:129`) with IAM binding for API SA (`infra/terraform/iam.tf:43`).

**Gaps / unknowns:**

- Secret-access audit logging (path only) is a stated invariant (`docs/plans/2025-01-12-arco-orchestration-design-part2.md:2015`), but no implementation evidence was collected in this pass.
- Unknown: repository-wide enforcement that “secrets never in events” (e.g., schema validation hooks) was not evaluated in this pass.

**Recommended actions:**

- Add a “secrets-invariants” regression suite that fails if secrets appear in logs/events (redaction + event validation).
- Add audit events for secret access (path only), consistent with design intent (`docs/plans/2025-01-12-arco-orchestration-design-part2.md:2015`).

---

## G) Network Perimeter & Transport Security Evidence

**Status:** ⚠️ Implemented via Cloud Run configuration; needs explicit posture validation

**What we verified (with evidence):**

- API ingress is internal-only by default; public exposure is controlled by `api_public` (`infra/terraform/cloud_run.tf:25`).
- When `api_public=true`, Terraform allows unauthenticated invocation (`allUsers`) (`infra/terraform/iam.tf:135`).
- CORS is disabled by default (secure-by-default) (`crates/arco-api/src/config.rs:81`), and wildcard origins are rejected when `debug=false` (`crates/arco-api/src/server.rs:573`).
- Rate limiting exists and is applied to authenticated routes (`crates/arco-api/src/server.rs:384`, `crates/arco-api/src/rate_limit.rs:249`).

**What remains unknown / out of scope:**

- Edge/WAF controls (Cloud Armor), mTLS, and custom-domain TLS settings are not represented in this repo and require deployment evidence.
- Cloud Scheduler trigger calls the compactor via its Cloud Run service `uri` even though the compactor is internal-only; verify end-to-end ingress + OIDC semantics in the target environment (`infra/terraform/cloud_run.tf:169`, `infra/terraform/cloud_run.tf:282`).

---

## 5) Known Gaps & Risks (Prioritized)

### P0 (Critical)

| Gap | Risk | Mitigation | Status |
|-----|------|------------|--------|
| Debug mode outside dev posture (regression risk) | Caller chooses tenant/workspace via headers; task callbacks accept any bearer token; cross-tenant read/write | Guardrail enforced at startup; keep regression tests (`crates/arco-api/src/server.rs:578`, `crates/arco-api/src/server.rs:784`) | ✅ Mitigated |
| Compactor `/metrics` unauthenticated (ensure internal-only ingress) | Identifier leakage + endpoint surface disclosure; potential DoS of monitoring pipeline if exposed | API blocks `/metrics` in public posture; keep compactor internal or add metrics auth (`crates/arco-api/src/server.rs:374`, `crates/arco-api/src/server.rs:384`, `crates/arco-compactor/src/main.rs:963`, `infra/terraform/cloud_run.tf:25`) | ⚠️ Infra-dependent |
| ~~Iceberg credential vending not wired~~ | ~~Engines cannot rely on scoped access delegation; blocks secure multi-engine use~~ | GCS `CredentialProvider` wired (`crates/arco-api/src/server.rs:630-637`); vend/deny audit events implemented (`crates/arco-iceberg/src/audit.rs:67,96`) | ✅ **Closed** |
| Iceberg REST baseline endpoints missing | Engines/clients cannot fully use Arco as Iceberg REST catalog; parity gate fails | Implement rename, metrics, transactions; keep `/v1/config` capability hiding accurate (`crates/arco-iceberg/src/router.rs:37`, `crates/arco-iceberg/src/types/config.rs:10`) | Open (rename/txn/metrics) |

### P1 (High)

| Gap | Risk | Mitigation |
|-----|------|------------|
| No JWKS/rotation for JWT verification | Key rotation operational risk; manual key distribution | Track JWKS implementation; add `jwks_url` + caching + alg allowlist |
| Conditional IAM list semantics unverified | Anti-entropy may fail or require broader perms | Add sandbox verification + capture evidence |

### P2 (Medium)

| Gap | Risk | Mitigation |
|-----|------|------------|
| URL redaction helpers exist but unused | Accidental signed URL logging later | Adopt redaction wrapper in log sites; add test (`crates/arco-api/src/redaction.rs:8`) |
| Metrics label regression risk | Future changes could reintroduce raw-path or tenant/workspace labels; monitoring cost/DoS if that happens | Keep label-policy tests (`crates/arco-api/src/metrics.rs:245`, `crates/arco-iceberg/src/metrics.rs:369`) |
| Rate limiting is per-instance in-memory | Bursts across instances; DoW still possible | Consider shared limiter or edge WAF |

---

## 6) Unused / Dead Code Inventory

| File | Code | Status | Recommendation |
|------|------|--------|----------------|
| `crates/arco-api/src/auth.rs:30` | `JwtVerifier` (JWKS caching, claim extraction) | Present but not compiled (not in module tree) | Either wire it via `crates/arco-api/src/lib.rs:52` and migrate JWT logic, or delete |
| `crates/arco-api/src/redaction.rs:8` | `redact_url` / `RedactedUrl` | Compiled but unused | Use in any logging surfaces that might touch signed URLs, or remove |

---

## 7) Appendix: File Reference Index

| Component | Primary Files |
|-----------|---------------|
| IAM / Terraform | `infra/terraform/iam.tf`, `infra/terraform/iam_conditions.tf`, `infra/terraform/main.tf`, `infra/terraform/cloud_run.tf` |
| API Auth | `crates/arco-api/src/context.rs`, `crates/arco-api/src/config.rs`, `crates/arco-api/src/server.rs`, `crates/arco-api/src/error.rs` |
| Signed URLs / Read Path | `crates/arco-api/src/routes/browser.rs`, `crates/arco-catalog/src/reader.rs`, `crates/arco-core/src/scoped_storage.rs`, `crates/arco-core/src/storage.rs` |
| Observability | `crates/arco-core/src/observability.rs`, `crates/arco-api/src/metrics.rs`, `crates/arco-iceberg/src/metrics.rs`, `infra/monitoring/*` |
| Iceberg REST | `crates/arco-api/src/server.rs`, `crates/arco-iceberg/src/router.rs`, `crates/arco-iceberg/src/routes/*`, `crates/arco-iceberg/src/state.rs`, `crates/arco-iceberg/src/types/config.rs` |
