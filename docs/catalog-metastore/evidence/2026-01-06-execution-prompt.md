# Execution Session Prompt — P0 Gap Closure

> **ARCHIVED — DO NOT EXECUTE**
>
> This document is a **historical artifact** from the 2026-01-06 P0 gap closure session.
> The prompt content below is preserved for audit trail purposes only.
>
> **Authoritative current status**: See [`2026-01-06-traceability-matrix.md`](./2026-01-06-traceability-matrix.md)
>
> Do not re-execute this prompt. Use the traceability matrix for current gap tracking.

---

**Generated**: 2026-01-06 (archived 2026-01-08)  
**Reference Docs**:
- `docs/catalog-metastore/evidence/2026-01-06-audit-review.md` (narrative review, historical)
- `docs/catalog-metastore/evidence/2026-01-06-traceability-matrix.md` (claim-by-claim status, **authoritative**)

---

## Copy-Paste Prompt

```
I have a comprehensive audit of our Security/Ops P0 and Iceberg Parity P0 implementation plan. The audit artifacts are:
- `docs/catalog-metastore/evidence/2026-01-06-audit-review.md` (narrative review)
- `docs/catalog-metastore/evidence/2026-01-06-traceability-matrix.md` (claim-by-claim status with 51 claims: 32 Implemented, 10 Partial, 9 Missing)

**Context**: We audited the plan from `docs/catalog-metastore/evidence/plan-deltas.md` which covers:
- Debug guardrails (1.1) - 5 claims
- JWT policy hardening (1.2) - 6 claims
- /metrics protection + label policy (1.3) - 5 claims
- Minimal structured audit events (1.4) - 7 claims
- Terraform public access prevention (1.5) - 2 claims
- IAM list semantics verification (1.6) - 4 claims
- Iceberg REST parity (2.1, 2.2) - 12 claims
- Credential vending (3) - 5 claims
- Engine interop readiness (4) - 4 claims

---

## P0 Gaps to Fix (in priority order)

### 1. Audit emission for credential vending (AUD-5) — MISSING

**Problem**: `AuditAction::CredVendAllow` and `AuditAction::CredVendDeny` exist at `crates/arco-core/src/audit.rs:58` but are never emitted.

**Fix location**: `crates/arco-iceberg/src/routes/tables.rs` around the `load_table_credentials` handler (line ~839-871).

**Requirements**:
- Emit `CredVendAllow` on successful credential vending (after `crates/arco-iceberg/src/routes/tables.rs:871`)
- Emit `CredVendDeny` on authorization failure or disabled vending (at `crates/arco-iceberg/src/routes/tables.rs:860`)
- Include: tenant_id, workspace_id, table identifier, request context
- NEVER include: actual credentials, signed URLs, tokens (per `crates/arco-core/src/audit.rs:106`)

**Pattern to follow**: See auth allow/deny in `crates/arco-api/src/audit.rs:44` and URL mint in `crates/arco-api/src/routes/browser.rs:208`.

### 2. Audit emission for Iceberg commits (AUD-6) — MISSING

**Problem**: `AuditAction::IcebergCommit` and `AuditAction::IcebergCommitDeny` exist at `crates/arco-core/src/audit.rs:62` but are never emitted. Current commit receipts (`crates/arco-iceberg/src/events.rs:18`) are forensic artifacts, not unified audit events.

**Fix location**: `crates/arco-iceberg/src/commit.rs` in the commit success/failure paths (~372, ~400).

**Requirements**:
- Emit `IcebergCommit` on successful table commit
- Emit `IcebergCommitDeny` on commit rejection (conflict, validation failure)
- Include: tenant_id, workspace_id, table identifier, snapshot summary
- NEVER include: actual data paths, partition values with PII

### 3. OpenAPI truthfulness: planId (OAS-3) — PARTIAL

**Problem**: `planId` is documented at `crates/arco-iceberg/src/routes/tables.rs:824` in the OpenAPI spec but the handler at `:839` doesn't parse it.

**Fix options** (pick one):
- **A)** Add `Query<CredentialsQuery>` extractor that includes `planId: Option<String>` and log/use it
- **B)** Remove `planId` from the OpenAPI documentation

**Recommendation**: Option A - parse and log it for observability. The Iceberg spec includes it for "plan-based" credential requests.

### 4. Flow metrics tenant label leakage (MET-5) — PARTIAL

**Problem**: `tenant` label at these locations leaks tenant identifiers and creates cardinality risk:
- `crates/arco-flow/src/metrics.rs:95` (label key definition)
- `crates/arco-flow/src/metrics.rs:142` (emission)
- `crates/arco-flow/src/orchestration/callbacks/handlers.rs:109` (callback metrics)

**Fix**: Remove the `tenant` label entirely, or replace with a hashed/bucketed value.

**Pre-check**: Review `infra/monitoring/dashboard.json` and `docs/runbooks/grafana-dashboard.json` to ensure no dashboards break.

### 5. IAM runbook script (IAM-4) — PARTIAL

**Problem**: `docs/runbooks/iam-list-semantics-verification.md:175` references `docs/runbooks/iam-list-verify.sh` which doesn't exist. The script content is embedded in markdown at `:41`.

**Fix**: Extract the embedded script (from `docs/runbooks/iam-list-semantics-verification.md:41`) into `docs/runbooks/iam-list-verify.sh` and make it executable.

### 6. Missing Iceberg endpoints (ICE-6, ICE-7, ICE-8) — ~~MISSING~~ **RESOLVED**

> **Resolution (2026-01-07)**:
> - **ICE-6 (rename)**: Implemented at `crates/arco-iceberg/src/routes/catalog.rs:59`. Advertised in `/v1/config` when `allow_table_crud` enabled. Within-namespace only; cross-namespace returns 406.
> - **ICE-7 (transactions)**: Partial — single-table bridge at `crates/arco-iceberg/src/routes/catalog.rs:132`. NOT advertised in `/v1/config` (interop bridge only). Multi-table atomic commit remains backlog.
> - **ICE-8 (metrics)**: Implemented at `crates/arco-iceberg/src/routes/tables.rs:999`. Uses official table-scoped path `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics` (not the catalog-level path shown below). Always advertised.
>
> See `2026-01-06-traceability-matrix.md` ICE-6/7/8 rows for authoritative current status.

These ~~are~~ were P0/P1 boundary. The ~~current~~ router at `crates/arco-iceberg/src/router.rs:31` ~~is limited to namespaces+tables~~ now includes catalog-level routes.

**~~Missing~~ Originally missing per Iceberg REST spec** (historical reference):
- `POST /v1/{prefix}/tables/rename` — Table rename endpoint (ICE-6) ✅
- `POST /v1/{prefix}/transactions/commit` — Multi-table atomic commit (ICE-7) ⚠️ single-table only
- `POST /v1/{prefix}/metrics` — Client metrics reporting (ICE-8, P1) ✅ (note: actual path is table-scoped)

**Note**: `/v1/config` at `crates/arco-iceberg/src/types/config.rs:44` ~~is currently truthful (doesn't advertise these). After implementing, update config to advertise them.~~ now advertises rename (when enabled) and metrics (always); transactions/commit is intentionally NOT advertised.

---

## Lower Priority (P1/Backlog)

| Gap | Claim ID | Evidence | Notes |
|-----|----------|----------|-------|
| pageToken is numeric offset, not opaque | OAS-4 | `crates/arco-iceberg/src/routes/utils.rs:57` | Document deviation or migrate to opaque |
| Nightly interop suite disabled | ENG-2 | `.github/workflows/nightly-chaos.yml:30` | All jobs `if: false` |
| JWKS not wired | JWT-6 | `crates/arco-api/src/lib.rs:52` | `auth.rs` exists but not compiled; explicit backlog |
| Compactor /metrics optional shared-secret gate | MET-4 | `crates/arco-compactor/src/main.rs:895-1003` | Gate enabled by non-empty (trimmed) `ARCO_METRICS_SECRET`; infra isolation remains primary defense in Cloud Run |

---

## Constraints (NON-NEGOTIABLE)

- Do NOT delete or weaken existing tests
- Do NOT implement JWKS (explicitly backlog per `docs/catalog-metastore/evidence/plan-deltas.md:234`)
- Audit events must NEVER contain secrets (JWTs, signed URLs, credentials) per `crates/arco-core/src/audit.rs:106`
- Keep `/v1/config` capability advertisement truthful — only advertise implemented endpoints
- Run `cargo clippy --workspace` and `cargo test --workspace` after each fix

---

## Evidence docs to update after fixes

After completing fixes, update these docs to reflect current state:
- `docs/catalog-metastore/evidence/security-ops-evidence-pack.md` — currently stale (claims credential vending not wired, but it is)
- `docs/catalog-metastore/evidence/plan-deltas.md` — mark completed items
- `docs/catalog-metastore/evidence/2026-01-06-traceability-matrix.md` — update claim statuses

---

## Execution Order

1. **Read** the audit review and traceability matrix first
2. **Fix AUD-5** (credential vending audit) — highest security impact
3. **Fix AUD-6** (Iceberg commit audit) — completes audit coverage
4. **Fix OAS-3** (planId truthfulness) — quick win
5. **Fix MET-5** (tenant label removal) — cardinality/privacy
6. **Fix IAM-4** (runbook script extraction) — enables verification
7. **Run full test suite** — `cargo test --workspace && cargo clippy --workspace`
8. **Update evidence docs** — reflect new state

For each fix, add appropriate unit tests. Mark the corresponding items in the traceability matrix as Implemented when done.
```

---

## Quick Reference: Key Files by Gap

| Gap ID | Claim | Primary File | Line(s) |
|--------|-------|--------------|---------|
| AUD-5 | Credential vending audit emission | `crates/arco-iceberg/src/routes/tables.rs` | ~839-871 |
| AUD-6 | Iceberg commit audit emission | `crates/arco-iceberg/src/commit.rs` | ~372, ~400 |
| OAS-3 | planId parameter parsing | `crates/arco-iceberg/src/routes/tables.rs` | ~824, ~839 |
| MET-5 | Tenant label in metrics | `crates/arco-flow/src/metrics.rs` | ~95, ~142 |
| MET-5 | Tenant label in callbacks | `crates/arco-flow/src/orchestration/callbacks/handlers.rs` | ~109 |
| IAM-4 | Runbook script extraction | `docs/runbooks/iam-list-semantics-verification.md` | ~41, ~175 |
| ICE-6 | Table rename endpoint | `crates/arco-iceberg/src/routes/catalog.rs` | 59 | ✅ Implemented |
| ICE-7 | Transactions commit endpoint | `crates/arco-iceberg/src/routes/catalog.rs` | 132 | ⚠️ Partial (single-table) |
| ICE-8 | Metrics endpoint | `crates/arco-iceberg/src/routes/tables.rs` | 999 | ✅ Implemented |

---

## Existing Patterns to Follow

| Pattern | Location | Use For |
|---------|----------|---------|
| Auth audit emission | `crates/arco-api/src/audit.rs:44` | AUD-5, AUD-6 structure |
| URL mint audit (avoid secrets) | `crates/arco-api/src/routes/browser.rs:167` | Secret-safe logging |
| Commit receipts | `crates/arco-iceberg/src/events.rs:18` | Forensic artifact format |
| Route handler with query params | `crates/arco-iceberg/src/routes/config.rs:34` | OAS-3 extractor pattern |

---

*Generated 2026-01-06*
