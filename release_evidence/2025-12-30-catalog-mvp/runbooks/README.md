# Runbooks Evidence

Date: 2025-12-31
Status: Resolved (maintainer self-signoff; external review deferred)

## Contents

- `blocker-runbook-signoffs.md` - Documents runbook presence and maintainer self-signoff (external review deferred)

## Optional Artifacts (for production readiness)

The following approval artifacts are required only if external SRE/Operations review is mandated:

1. **Runbook Review Approval** - One of:
   - Ticket (Jira, Linear, etc.) showing runbook review completed
   - PR approval showing runbook changes reviewed
   - Meeting notes from runbook review session

2. **On-Call Readiness Acknowledgment** - Evidence that SRE/Operations has:
   - Reviewed all runbooks for on-call usability
   - Confirmed escalation paths are correct
   - Verified incident procedures are actionable

## How to Complete External Review (if required)

1. Obtain runbook review approval artifact
2. Save artifact to this directory (e.g., `runbook-review-approval.pdf`, `oncall-ack.txt`)
3. Update `blocker-runbook-signoffs.md` to reference the artifact
4. Update Sign-Off Record in `docs/audits/2025-12-29-prod-readiness/README.md`

## Runbook Inventory

Reference: `release_evidence/2025-12-30-audit/runbooks/README.md`

| Runbook | Path | Status |
|---------|------|--------|
| Storage Integrity Verification | `docs/runbooks/storage-integrity-verification.md` | Present |
| GC Failure Handling | `docs/runbooks/gc-failure.md` | Present |
| Task Stuck Dispatched | `docs/runbooks/task-stuck-dispatched.md` | Present |
| High Task Failure Rate | `docs/runbooks/high-task-failure-rate.md` | Present |
| Scheduler Not Progressing | `docs/runbooks/scheduler-not-progressing.md` | Present |
| Metrics Catalog | `docs/runbooks/metrics-catalog.md` | Present |
| Prometheus Alerts | `docs/runbooks/prometheus-alerts.yaml` | Present |
| Performance Baseline | `docs/runbooks/perf-baseline.md` | Present |
| Soak Test | `docs/runbooks/soak-test.md` | Present |
| Rollback Drill | `docs/runbooks/rollback-drill.md` | Present |
| Pen Test Scope | `docs/runbooks/pen-test-scope.md` | Present |
