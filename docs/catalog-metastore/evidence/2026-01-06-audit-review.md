# Plan Execution Audit Review — 2026-01-06

> **⚠️ HISTORICAL SNAPSHOT**: This document captures the pre-fix gap analysis state.
> The P0 gaps identified here (AUD-5, AUD-6, MET-5, OAS-3, IAM-4) have been **resolved**.
> For current implementation status, see `2026-01-06-traceability-matrix.md` (updated).

**Auditor**: Automated codebase analysis (Sisyphus agent)  
**Scope**: Security/Ops P0, Iceberg Parity P0, Credential Vending P0, Engine Interop P0/P1  
**Methodology**: Parallel agent exploration + direct tool verification + Oracle guidance

---

## Baseline / Hygiene

- `git status --porcelain` and `git diff`: no local changes (both outputs empty in my capture).
- Evidence docs treated as requirements and cross-checked: `docs/catalog-metastore/evidence/security-ops-evidence-pack.md:1`, `docs/catalog-metastore/evidence/plan-deltas.md:1`.
- **Important note**: those evidence docs are dated 2026-01-02 and appear **stale vs current code** (e.g. they claim credential vending "not wired", but current `crates/arco-api/src/server.rs:630` does wire it).

---

## Security/Ops P0 Status

### 1.1 Debug guardrail: Implemented (with one footgun)

- Hard startup guard: `crates/arco-api/src/server.rs:702`.
- Header-based scoping allowed only in dev posture: `crates/arco-api/src/context.rs:80`, `crates/arco-api/src/routes/tasks.rs:609`.
- Terraform couples `ARCO_DEBUG` to `dev && !api_public`: `infra/terraform/cloud_run.tf:94`.
- **Footgun**: `arco-iceberg` accepts `X-Tenant-Id`/`X-Workspace-Id` whenever it's mounted without injected context (not posture-gated): `crates/arco-iceberg/src/context.rs:47`, `crates/arco-iceberg/src/context.rs:101`. This is safe in the intended mounting behind `arco-api`, but unsafe if exposed standalone.

### 1.2 JWT policy hardening: Implemented; task-callback parity needs a look

- Prod fail-closed for non-empty `iss`/`aud`: `crates/arco-api/src/server.rs:762`, `crates/arco-api/src/server.rs:774`.
- Empty env vars treated as unset (`env_string` trims + empty => None): `crates/arco-api/src/config.rs:518`.
- Runtime validation applies `iss`/`aud` when configured: `crates/arco-api/src/context.rs:125`, `crates/arco-api/src/context.rs:128`.
- **Known gap**: task-callback JWT validation sets issuer/audience, but doesn't mirror the explicit "claim must be present/non-empty" checks used in the main auth path (see the detailed logic in `crates/arco-api/src/context.rs:139` and compare to task middleware).

### 1.3 /metrics protection + label policy: Partial

- API `/metrics` is hidden (404) in public posture: `crates/arco-api/src/server.rs:442`, tested by `crates/arco-api/tests/api_integration.rs:110`.
- Compactor `/metrics` supports an optional shared-secret gate when `ARCO_METRICS_SECRET` is non-empty (trimmed): `crates/arco-compactor/src/main.rs:895-1003` (infra-only reachability remains the primary control in Cloud Run).
- API request metric labels are bounded via `MatchedPath` templates (no raw paths): `crates/arco-api/src/metrics.rs:90`.
- **High-cardinality / identifier leakage risk** still present in flow/orchestration metrics: `tenant` is a label key and is emitted in counters: `crates/arco-flow/src/metrics.rs:95`, `crates/arco-flow/src/metrics.rs:142`, and callback metrics include `tenant` label too: `crates/arco-flow/src/orchestration/callbacks/handlers.rs:109`.

### 1.4 Minimal structured audit events: Partial

- Stable structured audit schema exists (`AuditEvent`, `AuditAction`) and is secret-aware: `crates/arco-core/src/audit.rs:49`, `crates/arco-core/src/audit.rs:122`.
- Auth allow/deny events are emitted: `crates/arco-api/src/context.rs:316`, `crates/arco-api/src/context.rs:321` (builders in `crates/arco-api/src/audit.rs:44` and `crates/arco-api/src/audit.rs:61`).
- URL mint allow/deny events are emitted (and logs explicitly avoid signed URLs): `crates/arco-api/src/routes/browser.rs:27`, `crates/arco-api/src/routes/browser.rs:148`, `crates/arco-api/src/routes/browser.rs:186`, `crates/arco-api/src/routes/browser.rs:208`.
- **Credential vending + Iceberg commit are NOT emitted** as unified audit events today (the enum variants exist, but I found no emission callsites outside `crates/arco-core/src/audit.rs:58`).
- Iceberg commit does have structured "pending/committed receipt" artifacts (decision/outcome-ish) written best-effort: schema `crates/arco-iceberg/src/events.rs:18`, `crates/arco-iceberg/src/events.rs:49` and write points `crates/arco-iceberg/src/commit.rs:372`, `crates/arco-iceberg/src/commit.rs:400`.

### 1.5 Terraform public access prevention: Implemented

- Bucket `public_access_prevention = "enforced"`: `infra/terraform/main.tf:63` (+ UBLA: `infra/terraform/main.tf:58`).
- Public API exposure is explicit and gated by `api_public` (Cloud Run invoker to `allUsers`): `infra/terraform/iam.tf:135`.

### 1.6 IAM list semantics verification: Partial

- Runbook exists with concrete procedure and an evidence template: `docs/runbooks/iam-list-semantics-verification.md:3`, `docs/runbooks/iam-list-semantics-verification.md:188`.
- Not yet executed / evidence not captured (still called out as pending in `docs/catalog-metastore/evidence/security-ops-evidence-pack.md:139`).
- **Runbook has two correctness gaps**: it references a non-existent `docs/runbooks/iam-list-verify.sh` and uses an SA name that doesn't match Terraform's anti-entropy SA naming.

---

## Iceberg Parity P0 Status

### 2.1 Missing endpoints + semantics: ~~Partial~~ **Implemented** (updated 2026-01-07)

> **Update 2026-01-07**: ICE-6 (rename), ICE-8 (metrics) now implemented. ICE-7 (transactions) partial (single-table bridge).
> See `2026-01-06-traceability-matrix.md` ICE rows for authoritative status.

- **Implemented + routed**: namespaces CRUD/properties and tables create/drop/register/commit/credentials (see route wiring `crates/arco-iceberg/src/routes/namespaces.rs:40` and `crates/arco-iceberg/src/routes/tables.rs:41`).
- `/v1/config` capability advertisement is "truthful" and config-driven: `crates/arco-iceberg/src/types/config.rs:44`. ~~(note it does not advertise rename/transactions/metrics)~~ Now advertises rename (when enabled) and metrics (always); transactions NOT advertised (interop bridge only).
- `purgeRequested` behavior is minimal-but-correct (rejects purge): `crates/arco-iceberg/src/routes/tables.rs:764`.
- ~~**Still missing vs Iceberg spec checklist**: table rename, metrics reporting, and transactions commit.~~ **Now implemented**: rename (`crates/arco-iceberg/src/routes/catalog.rs:59`), metrics (`crates/arco-iceberg/src/routes/tables.rs:999`), transactions single-table bridge (`crates/arco-iceberg/src/routes/catalog.rs:132`).

### 2.2 Parameter correctness + OpenAPI alignment: Partial

- `warehouse` is parsed but ignored: `crates/arco-iceberg/src/routes/config.rs:34`.
- `pageToken` is treated as a numeric offset (not opaque): `crates/arco-iceberg/src/routes/utils.rs:57`.
- `planId` is documented in OpenAPI for `/credentials` but is not parsed by the handler (no `Query(...)` extractor): `crates/arco-iceberg/src/routes/tables.rs:824`, `crates/arco-iceberg/src/routes/tables.rs:839`.
- OpenAPI "compliance" test is spec-to-spec (param names + numeric response codes), not runtime behavior: `crates/arco-iceberg/tests/openapi_compliance.rs:116`.

---

## Credential Vending (P0)

- Provider exists + is feature-gated: GCS provider definition `crates/arco-iceberg/src/credentials/gcs.rs:49`, TTL clamp `crates/arco-iceberg/src/credentials/mod.rs:33`.
- Wiring is real in `arco-api` when `ARCO_ICEBERG_ENABLE_CREDENTIAL_VENDING=true`: `crates/arco-api/src/config.rs:448`, `crates/arco-api/src/server.rs:630`.
- **Still missing for the plan's deliverable bar**:
  - No `CRED_VEND_ALLOW/DENY` audit emission at the decision point (see audit section above).
  - No end-to-end API-layer integration test proving the provider is attached (note `Server::test_router()` disables credential vending): `crates/arco-api/src/server.rs:697`.

---

## Engine Interop Readiness (P0/P1 split)

- Compatibility matrix doc exists: `docs/iceberg-engine-compatibility.md:15` (Spark/Trino/PyIceberg marked "Tested", Flink "Untested": `docs/iceberg-engine-compatibility.md:19`).
- **Automated smoke/conformance suite: Missing/disabled**.
  - "Nightly chaos" workflow exists but all jobs are `if: false`: `.github/workflows/nightly-chaos.yml:30`, `.github/workflows/nightly-chaos.yml:65`, `.github/workflows/nightly-chaos.yml:89`.
- Current "interop-ish" test is a REST client test that mounts the Iceberg router directly and uses custom tenancy headers (not API JWT auth): `crates/arco-integration-tests/tests/iceberg_rest_catalog.rs:131`.

---

## Backlog / Remaining Gaps (highest leverage)

| Priority | Gap | Evidence | Suggested Fix |
|----------|-----|----------|---------------|
| P0 | Emit structured audit events for credential vending | `crates/arco-core/src/audit.rs:58` | Add `emit_cred_vend_allow/deny()` helpers; call from `maybe_vended_credentials()` |
| P0 | Emit structured audit events for Iceberg commits | `crates/arco-core/src/audit.rs:62` | Add emission in `CommitService::commit()` success/failure paths |
| P0 | OpenAPI/runtime mismatch: `planId` documented but ignored | `crates/arco-iceberg/src/routes/tables.rs:824` | Either parse it or remove from OpenAPI |
| P0 | OpenAPI/runtime mismatch: `pageToken` is numeric | `crates/arco-iceberg/src/routes/utils.rs:57` | Document deviation or switch to opaque tokens |
| P0 | Missing Iceberg endpoint: table rename | `crates/arco-iceberg/src/router.rs:31` | Implement `POST /v1/{prefix}/tables/rename` |
| P0 | Missing Iceberg endpoint: transactions commit | `crates/arco-iceberg/src/router.rs:31` | Implement `POST /v1/{prefix}/transactions/commit` |
| P1 | Missing Iceberg endpoint: metrics reporting | `crates/arco-iceberg/src/router.rs:31` | Implement metrics POST endpoint |
| P1 | Metrics label leakage: `tenant` in flow metrics | `crates/arco-flow/src/metrics.rs:95` | Remove or hash the `tenant` label |
| P1 | IAM list semantics: runbook not executable | `docs/runbooks/iam-list-semantics-verification.md:175` | Extract script to file; fix SA naming |
| Backlog | JWKS not wired | `crates/arco-api/src/lib.rs:52` | Wire `auth.rs` or remove the file |

---

## Traceability Matrix

See companion file: `2026-01-06-traceability-matrix.md`

---

## Next Steps

1. **Update stale evidence docs** to reflect current implementation state (especially credential vending wiring).
2. **Implement audit emission** for credential vending and Iceberg commits (P0).
3. **Fix OpenAPI truthfulness** for `planId` and pagination tokens.
4. **Add missing Iceberg endpoints** (rename, transactions) to reach spec parity.
5. **Enable nightly interop suite** with at least one engine (Spark or Trino).
6. **Execute IAM list semantics runbook** and capture evidence.
