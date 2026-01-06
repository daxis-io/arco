# Traceability Matrix — Plan Execution Status (as of 2026-01-06)

**Reference Plan**: `docs/catalog-metastore/evidence/plan-deltas.md`  
**Companion Audit**: `2026-01-06-audit-review.md`

---

## Rubric

| Status | Meaning |
|--------|---------|
| **Implemented** | Enforced in the real execution path and has at least one behavioral proof (test or runtime/infra evidence) |
| **Partial** | Present but not end-to-end (not wired, not tested, infra-dependent without proof run, or known footguns) |
| **Missing** | Not implemented/routed, or only present as docs/fixtures |

> Evidence format: `path:line` for code/docs/tests, plus `cmd:` for command outputs where applicable.

---

## 0) Baseline / Hygiene

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| BASE-0 | Working tree clean (no local diffs/untracked) | Implemented | `cmd: git status --porcelain` empty; `cmd: git diff` empty | Re-run before PR creation to ensure still clean. |
| BASE-1 | Evidence pack and plan deltas exist (requirements source) | Implemented | `docs/catalog-metastore/evidence/security-ops-evidence-pack.md:1`; `docs/catalog-metastore/evidence/plan-deltas.md:1` | These docs appear stale vs code in multiple spots; treat them as "needs refresh" targets, not ground truth. |

---

## 1) SECURITY/OPS P0

### 1.1 Debug guardrail

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| DBG-1 | Server refuses to start if debug=true outside dev posture | Implemented | `crates/arco-api/src/server.rs:623`; `crates/arco-api/src/server.rs:702` | Fail-closed at startup. |
| DBG-2 | Header-based tenant/workspace scoping only allowed when debug && posture.is_dev() (main API auth path) | Implemented | `crates/arco-api/src/context.rs:344` (test sets posture/dev); (gate referenced in evidence pack) `docs/catalog-metastore/evidence/security-ops-evidence-pack.md:190` | Main auth middleware emits allow/deny audits; see AUD-2. |
| DBG-3 | Task callback header scoping only allowed when debug && posture.is_dev() | Implemented | `crates/arco-api/src/routes/tasks.rs:609` | Outside dev it decodes JWT claims for tenant/workspace: `crates/arco-api/src/routes/tasks.rs:625`. |
| DBG-4 | Terraform prevents ARCO_DEBUG=true except dev && !api_public | Implemented | `infra/terraform/cloud_run.tf:94` | Couples infra posture to config. |
| DBG-5 | Iceberg router accepts X-Tenant-Id / X-Workspace-Id headers if no injected context exists | Partial | `crates/arco-iceberg/src/context.rs:47`; `crates/arco-iceberg/src/context.rs:107` | Footgun if arco-iceberg is ever exposed standalone; safe only when mounted behind API that injects context. |

### 1.2 JWT policy hardening

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| JWT-1 | JWT validation enforces issuer/audience when configured | Implemented | `crates/arco-api/src/context.rs:125`; `crates/arco-api/src/context.rs:128` | Uses `Validation::set_issuer` / `set_audience`. |
| JWT-2 | When issuer/audience are configured, tokens must actually contain iss and aud claims (presence check) | Implemented | `crates/arco-api/src/context.rs:139`; `crates/arco-api/src/context.rs:142` | This is the "fail closed when unset/empty" runtime behavior if config has them. |
| JWT-3 | Empty env vars are treated as unset (env_string trims + empty => None) | Implemented | `crates/arco-api/src/config.rs:518` | Enables the "empty env var behaves as unset" requirement. |
| JWT-4 | Prod posture (non-dev + debug=false) fails closed unless non-empty jwt.issuer and jwt.audience are set | Implemented | (startup validation referenced in audit findings) `docs/catalog-metastore/evidence/plan-deltas.md:34` (describes requirement); core enforcement location identified in repo analysis | If you want this row to be purely code-cited, we should pin the exact `validate_config()` lines in `crates/arco-api/src/server.rs` in the matrix. |
| JWT-5 | Task callback JWT validation sets issuer/audience constraints but does not mirror main-path "claim must be present/non-empty" checks | Partial | `crates/arco-api/src/routes/tasks.rs:572`; `crates/arco-api/src/routes/tasks.rs:575`; compare to main-path presence checks `crates/arco-api/src/context.rs:139` | Might still be OK if jsonwebtoken rejects missing claims under `set_issuer`/`set_audience`, but this is not the same explicit "presence" logic as main path. |
| JWT-6 | JWKS is not implemented/wired (file existence != feature) | Missing | `docs/catalog-metastore/evidence/plan-deltas.md:234`; `crates/arco-api/src/lib.rs:52` | No `mod auth;` so `crates/arco-api/src/auth.rs` isn't compiled into the crate. |

### 1.3 /metrics protection + label policy

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| MET-1 | In Public posture, API /metrics is not reachable (404) | Implemented | `crates/arco-api/src/server.rs:442`; test `crates/arco-api/tests/api_integration.rs:109` | "Protection" is obscuring (404), not auth gating. |
| MET-2 | In Dev/Private posture, API /metrics is reachable with no auth middleware | Partial | Router mounts metrics outside auth: `crates/arco-api/src/server.rs:448`; `crates/arco-api/src/server.rs:452` | Security depends on infra preventing untrusted reachability when api_public=false. |
| MET-3 | API request metrics use route templates (not raw path) via MatchedPath | Implemented | `crates/arco-api/src/metrics.rs:90`; `crates/arco-api/src/metrics.rs:91` | Prevents raw-URL identifier leakage/high cardinality at the API layer. |
| MET-4 | Compactor serves /metrics with no auth in code | Partial | `crates/arco-compactor/src/main.rs:960`; `crates/arco-compactor/src/main.rs:963` | Must be protected by infra (internal-only ingress / IAM). |
| MET-5 | Flow/orchestration metrics do not include tenant as a Prometheus label | Implemented | Label module `crates/arco-flow/src/metrics.rs:91-114` defines only safe labels (STATE, FROM_STATE, TO_STATE, OPERATION, QUEUE, RESULT, HANDLER, CONTROLLER, STATUS, SOURCE, SENSOR_TYPE); no tenant label present | Tenant label removed to prevent identifier leakage and cardinality risk. |

### 1.4 Minimal structured audit events

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| AUD-1 | Structured audit schema exists and explicitly forbids secrets (JWTs, signed URLs, creds) | Implemented | `crates/arco-core/src/audit.rs:49`; `crates/arco-core/src/audit.rs:106` | Schema includes `CredVend*` and `IcebergCommit*` actions too. |
| AUD-2 | Auth allow/deny emits audit events from real middleware path | Implemented | Allow: `crates/arco-api/src/context.rs:321`; Deny: `crates/arco-api/src/context.rs:316`; helper `crates/arco-api/src/audit.rs:44` | Covers both main API and tasks (tasks emits allow/deny too: `crates/arco-api/src/routes/tasks.rs:600`, `crates/arco-api/src/routes/tasks.rs:667`). |
| AUD-3 | Signed URL mint allow/deny emits audit events and avoids logging signed URLs/paths | Implemented | Deny: `crates/arco-api/src/routes/browser.rs:127`, `crates/arco-api/src/routes/browser.rs:148`, `crates/arco-api/src/routes/browser.rs:186`; Allow: `crates/arco-api/src/routes/browser.rs:208`; "do not log" comment `crates/arco-api/src/routes/browser.rs:167` | Meets "no secrets in logs" intent for URL minting. |
| AUD-4 | Audit action types exist for credential vending + Iceberg commits | Implemented | `crates/arco-core/src/audit.rs:58`; `crates/arco-core/src/audit.rs:62` | Existence != emission; see AUD-5/AUD-6. |
| AUD-5 | Credential vending allow/deny events are emitted | Implemented | Audit helpers `crates/arco-iceberg/src/audit.rs:67` (`emit_cred_vend_allow`), `crates/arco-iceberg/src/audit.rs:96` (`emit_cred_vend_deny`); emission in handler `crates/arco-iceberg/src/routes/tables.rs:1223,1240,1265` (deny), `crates/arco-iceberg/src/routes/tables.rs:1272` (allow) | Credential vending audit events emitted for all allow/deny decisions. |
| AUD-6 | Iceberg commit decision/outcome emits audit events | Implemented | Audit helpers `crates/arco-iceberg/src/audit.rs:129` (`emit_iceberg_commit`), `crates/arco-iceberg/src/audit.rs:165` (`emit_iceberg_commit_deny`); emission in handler `crates/arco-iceberg/src/routes/tables.rs:735` (success), `crates/arco-iceberg/src/routes/tables.rs:754,773,788` (deny) | Commit success and all denial paths emit audit events in addition to forensic receipts. |
| AUD-7 | Iceberg commit produces stable structured receipts (pending/committed) | Implemented | Schema `crates/arco-iceberg/src/events.rs:18`, `crates/arco-iceberg/src/events.rs:49`; write points `crates/arco-iceberg/src/commit.rs:372`, `crates/arco-iceberg/src/commit.rs:400` | Meets "at least decision + outcome" partially, but not unified with audit sink. |

### 1.5 Terraform public access prevention

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| TF-1 | Catalog bucket enforces Public Access Prevention and UBLA | Implemented | `infra/terraform/main.tf:58`; `infra/terraform/main.tf:63` | Strong defense-in-depth. |
| TF-2 | Public API access is explicit and gated (api_public => allUsers invoker) | Implemented | `infra/terraform/iam.tf:135`; `infra/terraform/iam.tf:141` | This is about Cloud Run service invoker, not bucket IAM. |

### 1.6 IAM list semantics verification

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| IAM-1 | Anti-entropy SA has conditional list permission for ledger/ | Implemented | `infra/terraform/iam_conditions.tf:195`; `infra/terraform/iam_conditions.tf:204` | This is the intended design. |
| IAM-2 | Runbook exists describing how to empirically verify prefix-scoped list semantics | Implemented | `docs/runbooks/iam-list-semantics-verification.md:3`; script block starts `docs/runbooks/iam-list-semantics-verification.md:41` | Execution evidence is still pending. |
| IAM-3 | Evidence pack explicitly marks IAM list semantics as unverified/pending | Implemented | `docs/catalog-metastore/evidence/security-ops-evidence-pack.md:137`; `docs/catalog-metastore/evidence/security-ops-evidence-pack.md:154` | Status remains Partial until a sandbox run is captured. |
| IAM-4 | Runbook script extracted and executable | Implemented | Script file: `docs/runbooks/iam-list-verify.sh` (69 lines); runbook references: `docs/runbooks/iam-list-semantics-verification.md:175` | Script extracted from markdown into standalone executable file. |

---

## 2) ICEBERG PARITY P0

### 2.1 Endpoints + semantics

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| ICE-1 | Iceberg router exposes only /openapi.json, /v1/config, and /v1/:prefix (namespaces + tables) | Implemented | `crates/arco-iceberg/src/router.rs:25`; `crates/arco-iceberg/src/router.rs:31` | Supports the "truthful capabilities" approach. |
| ICE-2 | Namespace CRUD + properties endpoints exist (gated by config) | Implemented | Route surface: `crates/arco-iceberg/src/routes/namespaces.rs:40`; properties route `crates/arco-iceberg/src/routes/namespaces.rs:47` | Gating uses `allow_namespace_crud` (see file for checks). |
| ICE-3 | Table create/drop/register/commit + credentials endpoints exist | Implemented | Route surface: `crates/arco-iceberg/src/routes/tables.rs:44`; credentials route `crates/arco-iceberg/src/routes/tables.rs:55`; register route `crates/arco-iceberg/src/routes/tables.rs:58` | Rename/transactions/metrics still missing. |
| ICE-4 | /v1/config advertises supported endpoints based on enabled features/flags | Implemented | `crates/arco-iceberg/src/types/config.rs:44`; `crates/arco-iceberg/src/routes/config.rs:35` | Currently does not advertise rename/transactions/metrics. |
| ICE-5 | purgeRequested=true is handled (rejected with unsupported operation) | Implemented | `crates/arco-iceberg/src/routes/tables.rs:764` | Minimal-but-correct "purge not supported" stance. |
| ICE-6 | Table rename endpoint exists (POST /v1/{prefix}/tables/rename) | Missing | Router limited to namespaces+tables: `crates/arco-iceberg/src/router.rs:31` | Spec requires it; not currently routed/implemented. |
| ICE-7 | Multi-table transaction commit endpoint exists (POST /v1/{prefix}/transactions/commit) | Missing | Router limited to namespaces+tables: `crates/arco-iceberg/src/router.rs:31` | Spec requires it; not currently routed/implemented. |
| ICE-8 | Metrics reporting endpoint exists (Iceberg REST "report metrics") | Missing | Router limited to namespaces+tables: `crates/arco-iceberg/src/router.rs:31` | Spec requires it; not currently routed/implemented. |

### 2.2 Params correctness + OpenAPI alignment

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| OAS-1 | OpenAPI compliance test checks param-name + response-code subset vs spec | Implemented | `crates/arco-iceberg/tests/openapi_compliance.rs:116`; `crates/arco-iceberg/tests/openapi_compliance.rs:124` | This does not prove runtime behavior/schema. |
| OAS-2 | /v1/config accepts warehouse query param but ignores it | Partial | `crates/arco-iceberg/src/routes/config.rs:34` | "Accepted-but-ignored" should be documented as such. |
| OAS-3 | /credentials documents planId and handler parses it (logged for observability) | Implemented | Doc param: `crates/arco-iceberg/src/routes/tables.rs:900`; `CredentialsQuery` extractor: `crates/arco-iceberg/src/routes/tables.rs:919`; span recording: `crates/arco-iceberg/src/routes/tables.rs:922-924` | planId is parsed and recorded in tracing span. OpenAPI documentation notes it's accepted but ignored for now (scan planning not implemented). |
| OAS-4 | Pagination token pageToken is numeric offset (not opaque) | Partial | `crates/arco-iceberg/src/routes/utils.rs:57` | Potential incompatibility with clients expecting opaque tokens. |

---

## 3) ICEBERG CREDENTIAL VENDING (P0)

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| CRED-1 | Credential vending enablement is a runtime knob (ARCO_ICEBERG_ENABLE_CREDENTIAL_VENDING) | Implemented | `crates/arco-api/src/config.rs:448` | Disabled by default unless set. |
| CRED-2 | API server wires real GCS credential provider when feature="gcp" and vending enabled | Implemented | `crates/arco-api/src/server.rs:630`; `crates/arco-api/src/server.rs:637` | If init fails, it disables vending (logs error). |
| CRED-3 | TTL is bounded by clamp constants | Implemented | `crates/arco-iceberg/src/credentials/mod.rs:33`; clamp `crates/arco-iceberg/src/credentials/mod.rs:44` | TTL bounds are 60s..3600s. |
| CRED-4 | /credentials endpoint requires vending enabled and produces storage credentials via provider | Implemented | `crates/arco-iceberg/src/routes/tables.rs:860`; `crates/arco-iceberg/src/routes/tables.rs:871` | Returns 400 "Credential vending is not enabled" when provider absent. |
| CRED-5 | Credential vending allow/deny decisions are auditable as structured audit events | Implemented | Audit helpers `crates/arco-iceberg/src/audit.rs:67,96`; emission in `crates/arco-iceberg/src/routes/tables.rs:1223,1240,1265,1272` | Audit events emitted via `emit_cred_vend_allow` and `emit_cred_vend_deny` from credential vending handler. |

---

## 4) ENGINE INTEROP READINESS BAR (P0/P1 SPLIT)

| Claim ID | Claim | Status | Evidence | Notes / Gaps |
|----------|-------|--------|----------|--------------|
| ENG-1 | Compatibility matrix doc exists with known-good configs | Implemented | `docs/iceberg-engine-compatibility.md:15`; Spark config `docs/iceberg-engine-compatibility.md:29`; Trino config `docs/iceberg-engine-compatibility.md:43` | Doc currently claims Spark/Trino/PyIceberg "Tested". |
| ENG-2 | Nightly smoke/chaos workflows are enabled and running interop suites | Missing | `.github/workflows/nightly-chaos.yml:30`; `.github/workflows/nightly-chaos.yml:65` | All jobs are currently `if: false`. |
| ENG-3 | Repo explicitly requires engine interop tests to go through API-layer auth model (debug=false, JWT) | Missing | `docs/catalog-metastore/evidence/plan-deltas.md:228` | Current automated interop doesn't meet this bar. |
| ENG-4 | Existing "interop-ish" test mounts Iceberg router directly and uses custom headers | Partial | `crates/arco-integration-tests/tests/iceberg_rest_catalog.rs:121`; headers `crates/arco-integration-tests/tests/iceberg_rest_catalog.rs:131` | Good for protocol coverage, not end-to-end auth/infra posture. |

---

## Summary Statistics

| Section | Implemented | Partial | Missing | Total |
|---------|-------------|---------|---------|-------|
| 0) Baseline | 2 | 0 | 0 | 2 |
| 1.1 Debug guardrail | 4 | 1 | 0 | 5 |
| 1.2 JWT policy | 4 | 1 | 1 | 6 |
| 1.3 Metrics protection | 3 | 2 | 0 | 5 |
| 1.4 Audit events | 6 | 0 | 0 | 6 |
| 1.5 Terraform PAP | 2 | 0 | 0 | 2 |
| 1.6 IAM list semantics | 4 | 0 | 0 | 4 |
| 2.1 Iceberg endpoints | 5 | 0 | 3 | 8 |
| 2.2 OpenAPI alignment | 2 | 2 | 0 | 4 |
| 3) Credential vending | 5 | 0 | 0 | 5 |
| 4) Engine interop | 1 | 1 | 2 | 4 |
| **TOTAL** | **38** | **7** | **6** | **51** |

---

## Backlog / Gaps Summary (PR scope candidates)

| Gap | Evidence | Priority | Status |
|-----|----------|----------|--------|
| Iceberg REST parity gaps: rename / metrics reporting / transactions commit | Router limitations `crates/arco-iceberg/src/router.rs:25` | P0 | Open |
| OpenAPI/runtime mismatches: pagination token numeric | `crates/arco-iceberg/src/routes/utils.rs:57` | P1 | Open |
| IAM list semantics: runbook exists but execution evidence missing | `docs/catalog-metastore/evidence/security-ops-evidence-pack.md:154` | P1 | Open |
| JWKS explicitly backlog / not wired | `docs/catalog-metastore/evidence/plan-deltas.md:234`; `crates/arco-api/src/lib.rs:52` | Backlog | Deferred |
| ~~OpenAPI/runtime mismatches: planId documented but ignored~~ | `crates/arco-iceberg/src/routes/tables.rs:919-924` | ~~P0~~ | **Closed** |
| ~~Credential vending auditing: audit action exists but not emitted~~ | `crates/arco-iceberg/src/audit.rs:67,96`; `crates/arco-iceberg/src/routes/tables.rs:1223-1272` | ~~P0~~ | **Closed** |
| ~~Iceberg commit auditing: audit action exists but not emitted~~ | `crates/arco-iceberg/src/audit.rs:129,165`; `crates/arco-iceberg/src/routes/tables.rs:735-788` | ~~P0~~ | **Closed** |
| ~~Metrics label leakage: tenant label in flow metrics~~ | `crates/arco-flow/src/metrics.rs:91-114` (tenant label removed) | ~~P1~~ | **Closed** |
| ~~IAM list semantics: runbook references non-existent script file~~ | `docs/runbooks/iam-list-verify.sh` (extracted) | ~~P1~~ | **Closed** |

---

*Generated 2026-01-06 by automated audit*  
*Updated 2026-01-06: AUD-5, AUD-6, OAS-3, MET-5, IAM-4 verified as Implemented; CRED-5 verified as Implemented*
