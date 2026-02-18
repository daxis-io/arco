# Gate 5 External Handoff Checklist

Generated UTC: 2026-02-15T19:29:45Z

| Signal | Owner | Command/Action | Expected artifact | Destination |
|---|---|---|---|---|
| G5-001 | Perf Engineering | `command -v k6 && command -v vegeta && command -v locust`; `TARGET_URL=<staging-url> k6 run <k6_10x_profile.js> --env TARGET_URL=$TARGET_URL`; `TARGET_URL=<staging-url> k6 run <k6_soak_profile.js> --duration 24h --env TARGET_URL=$TARGET_URL` | load/soak logs + SLO/error-budget exports | `gate-5/perf/` |
| G5-002 | SRE + Product | Update approval rows in `g5-002-retention-cost-control-policy.md` with approver names + UTC timestamps | signed approval record | `gate-5/g5-002-retention-cost-control-policy.md` |
| G5-003 | Security Engineering | Execute scoped pen-test from `docs/runbooks/pen-test-scope.md`; attach report; update triage matrix with remediation owners/ETAs | signed report + triage + remediation status | `gate-5/security/` |
| G5-004 | Security + Platform | Configure GitHub OIDC vars (`GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_SERVICE_ACCOUNT`), run `iam-smoke` on `main`, and attach security approval | CI run proof + signed policy approval | `gate-5/security/` |
| G5-005 | SRE | `gcloud auth login && gcloud auth application-default login`; `PROJECT_ID=<project> ./scripts/rollback.sh --env staging --include-compactor`; `ARCO_STORAGE_BUCKET=<bucket> cargo xtask verify-integrity --tenant=<tenant> --workspace=<workspace>` | rollback transcript + health + integrity report | `gate-5/rollback/` |
| G5-006 | Release Engineering | Fill Engineering/SRE/Security signoff rows in `g5-006-release-notes-signoff-pack.md` | signed signoff matrix | `gate-5/signoff/g5-006-release-notes-signoff-pack.md` |
