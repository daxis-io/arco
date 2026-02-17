# Gate 5 + Gate 7 Blocked Steps Tracker

Last updated UTC: 2026-02-15T19:35:34Z
Scope: unresolved Gate 5 and Gate 7 closure steps for end-of-cycle revisit.
Source status: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json`

## Active Blockers

| Signal | Gate | Status | Owner | Blocking reason | Required command/action | Expected artifact | Destination | Current blocker evidence |
|---|---:|---|---|---|---|---|---|---|
| G5-001 | 5 | BLOCKED-EXTERNAL | Perf Engineering | Staging soak/load not executed; load tools unavailable in this environment | `command -v k6 && command -v vegeta && command -v locust`; `TARGET_URL=<staging-url> k6 run <k6_10x_profile.js> --env TARGET_URL=$TARGET_URL`; `TARGET_URL=<staging-url> k6 run <k6_soak_profile.js> --duration 24h --env TARGET_URL=$TARGET_URL` | load/soak logs + SLO/error-budget exports | `release_evidence/2026-02-12-prod-readiness/gate-5/perf/` | `release_evidence/2026-02-12-prod-readiness/gate-5/perf/g5-001-soak-load-evidence.md` |
| G5-002 | 5 | BLOCKED-EXTERNAL | SRE + Product | Approval signatures missing | Update approval rows in `g5-002-retention-cost-control-policy.md` with approver + UTC timestamp | signed approval record | `release_evidence/2026-02-12-prod-readiness/gate-5/g5-002-retention-cost-control-policy.md` | `release_evidence/2026-02-12-prod-readiness/gate-5/g5-002-retention-cost-control-policy.md` |
| G5-003 | 5 | BLOCKED-EXTERNAL | Security Engineering | Pen-test execution/triage/remediation not completed | Execute scoped pen-test per `docs/runbooks/pen-test-scope.md`; attach report; update triage/remediation matrix | signed report + triage + remediation status | `release_evidence/2026-02-12-prod-readiness/gate-5/security/` | `release_evidence/2026-02-12-prod-readiness/gate-5/security/g5-003-pen-test-evidence.md` |
| G5-004 | 5 | BLOCKED-EXTERNAL | Security + Platform | Repo migration done; GitHub variable setup + successful OIDC smoke run + security approval pending | Configure `GCP_WORKLOAD_IDENTITY_PROVIDER` and `GCP_SERVICE_ACCOUNT`; run `iam-smoke` on `main`; attach security approval | CI run proof + signed policy approval | `release_evidence/2026-02-12-prod-readiness/gate-5/security/` | `release_evidence/2026-02-12-prod-readiness/gate-5/security/g5-004-secrets-rotation-policy-evidence.md` |
| G5-005 | 5 | BLOCKED-EXTERNAL | SRE | Live rollback drill not executed; non-interactive cloud auth failed in current session | `gcloud auth login && gcloud auth application-default login`; `PROJECT_ID=<project> ./scripts/rollback.sh --env staging --include-compactor`; `ARCO_STORAGE_BUCKET=<bucket> cargo xtask verify-integrity --tenant=<tenant> --workspace=<workspace>` | rollback transcript + health + integrity report | `release_evidence/2026-02-12-prod-readiness/gate-5/rollback/` | `release_evidence/2026-02-12-prod-readiness/gate-5/rollback/g5-005-rollback-drill-evidence.md` |
| G5-006 | 5 | BLOCKED-EXTERNAL | Release Engineering | Cross-functional signoffs missing | Fill Engineering/SRE/Security signoff rows in `g5-006-release-notes-signoff-pack.md` | signed signoff matrix | `release_evidence/2026-02-12-prod-readiness/gate-5/signoff/g5-006-release-notes-signoff-pack.md` | `release_evidence/2026-02-12-prod-readiness/gate-5/signoff/g5-006-release-notes-signoff-pack.md` |
| G7-001 | 7 | BLOCKED-EXTERNAL | Release Engineering + SRE | Production canary progression not executed | `gcloud auth login`; run `gcloud run services update-traffic ...` for `5% -> 25% -> 100%`; capture health gates between shifts | canary progression transcript + gate decisions | `release_evidence/2026-02-12-prod-readiness/final-go/` | `release_evidence/2026-02-12-prod-readiness/final-go/g7-001-canary-progression-evidence.md` |
| G7-002 | 7 | BLOCKED-EXTERNAL | Daxis Integrations | Production contract validation not executed | Export production auth/context vars and run discovery/query/admin checks with captured responses | production integration validation report | `release_evidence/2026-02-12-prod-readiness/final-go/` | `release_evidence/2026-02-12-prod-readiness/final-go/g7-002-production-integration-validation-evidence.md` |
| G7-003 | 7 | NO-GO | Product + Release | Messaging rule blocked until all required gates are GO | Run `jq -e '[.gates[] | select(.status != "GO")] | length == 0' docs/audits/2026-02-12-prod-readiness/gate-tracker.json` and only publish if it succeeds | messaging diff + publication record | `release_evidence/2026-02-12-prod-readiness/final-go/g7-003-messaging-update-gate.md` | `release_evidence/2026-02-12-prod-readiness/final-go/g7-003-messaging-update-gate.md` |

## End-of-Cycle Revisit Checklist

- [ ] G5-001 closed
- [ ] G5-002 closed
- [ ] G5-003 closed
- [ ] G5-004 closed
- [ ] G5-005 closed
- [ ] G5-006 closed
- [ ] G7-001 closed
- [ ] G7-002 closed
- [ ] G7-003 closed (only after all required gates are GO)

## Quick Links

- Gate 5 handoff checklist: `release_evidence/2026-02-12-prod-readiness/gate-5/external-handoff-checklist.md`
- Gate 7 handoff checklist: `release_evidence/2026-02-12-prod-readiness/final-go/external-handoff-checklist.md`
- Machine-checkable tracker: `docs/audits/2026-02-12-prod-readiness/gate-tracker.json`
