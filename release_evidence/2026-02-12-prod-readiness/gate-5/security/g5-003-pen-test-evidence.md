# G5-003 Pen-Test Evidence + Triage

Generated UTC: 2026-02-15T19:29:45Z
Status: BLOCKED-EXTERNAL
Owner: Security Engineering

## Closure Requirement

Execute scoped penetration test for release surfaces and attach findings + triage/remediation status.

## Fresh Evidence Captured (Local)

1. Pen-test scope runbook is present and defines in-scope APIs/storage and out-of-scope boundaries.
2. Automated advisory baseline check passed:
   - `cargo deny check advisories` => `advisories ok`
3. Triage matrix in this artifact still has unresolved `[PENDING]` placeholders.

## Why This Signal Is External

External/human security execution is required for the actual penetration test window, evidence signoff, and remediation confirmation.

## Findings/Triage Tracker (to be filled by Security)

| Finding ID | Severity | Surface | Status | Remediation owner | ETA | Evidence |
|---|---|---|---|---|---|---|
| [PENDING] | [PENDING] | [PENDING] | [PENDING] | [PENDING] | [PENDING] | [PENDING] |

## Evidence Artifacts

- `release_evidence/2026-02-12-prod-readiness/gate-5/security/g5-003-command-status.tsv`
- `release_evidence/2026-02-12-prod-readiness/gate-5/security/command-logs/20260215T192640Z_pen_test_runbook_presence_refresh.log`
- `release_evidence/2026-02-12-prod-readiness/gate-5/security/command-logs/20260215T192640Z_cargo_deny_advisories_refresh.log`
- `release_evidence/2026-02-12-prod-readiness/gate-5/security/command-logs/20260215T192641Z_pen_test_pending_findings_scan.log`

## External Handoff Steps

| Step | Owner | Command/Action | Expected artifact | Destination |
|---|---|---|---|---|
| Approve scope + window | Security Engineering | finalize scope from `docs/runbooks/pen-test-scope.md` and attach signatures | signed scope | `gate-5/security/pen_test_scope_signed.pdf` |
| Execute pen-test | External tester + Security | run approved test plan against staging/prod-approved target | raw report | `gate-5/security/pen_test_report_raw.pdf` |
| Triage findings | Security Engineering | update triage table in this file with Finding ID, severity, owner, ETA | triage matrix | `gate-5/security/g5-003-pen-test-evidence.md` |
| Verify remediation closure | Security + owning teams | rerun verification for fixed findings and record closure evidence | closure report | `gate-5/security/pen_test_remediation_closure.md` |
