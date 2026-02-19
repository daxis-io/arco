# G5-002 Retention + Cost-Control Policy

Generated UTC: 2026-02-15T19:29:45Z
Status: BLOCKED-EXTERNAL
Owners: SRE + Product

## Policy Decision

This release train uses a tiered retention policy backed by `RetentionPolicy` in `crates/arco-catalog/src/gc/policy.rs`.

| Profile | keep_snapshots | delay_hours | ledger_retention_hours | max_age_days | Intended usage |
|---|---:|---:|---:|---:|---|
| Development | 3 | 1 | 2 | 7 | local/dev test loops |
| Production default | 10 | 24 | 48 | 90 | standard production |
| Cost-sensitive | 5 | 12 | 24 | 30 | constrained environments |
| Compliance-heavy | 30 | 48 | 168 | 365 | regulated workloads |

## Cost-Control Guardrails

1. GC must run continuously and alert on failures.
2. Storage growth alerts must page when sustained growth exceeds threshold.
3. Any retention override requires SRE + Product approval and rollback window review.
4. Monthly retention review must confirm cost trend and recovery objectives remain met.

## Verification Evidence (Fresh)

1. `cargo test -p arco-catalog test_default_policy --lib` passed.
2. `cargo test -p arco-catalog test_valid_policy --lib` passed.
3. Source scan confirms production default values and aggressive/conservative profiles.

Artifacts:
- `release_evidence/2026-02-12-prod-readiness/gate-5/g5-002-command-status.tsv`
- `release_evidence/2026-02-12-prod-readiness/gate-5/command-logs/20260215T192612Z_retention_policy_default_test_refresh.log`
- `release_evidence/2026-02-12-prod-readiness/gate-5/command-logs/20260215T192624Z_retention_policy_valid_test_refresh.log`
- `release_evidence/2026-02-12-prod-readiness/gate-5/command-logs/20260215T192624Z_retention_policy_approval_state_scan.log`

## Approval State

Policy content is complete and validated, but required release approval signatures are not yet recorded.

| Role | Required approver | Status |
|---|---|---|
| SRE | [PENDING] | pending |
| Product | [PENDING] | pending |

## Completion Condition

Set to GO after SRE + Product approvals are attached to this artifact and linked in the Gate 5 signoff pack.

## External Handoff Steps

| Step | Owner | Command/Action | Expected artifact | Destination |
|---|---|---|---|---|
| SRE approval signature | SRE | update approval row in this file with approver name + UTC timestamp | signed SRE approval row | `gate-5/g5-002-retention-cost-control-policy.md` |
| Product approval signature | Product | update approval row in this file with approver name + UTC timestamp | signed Product approval row | `gate-5/g5-002-retention-cost-control-policy.md` |
| Link into signoff pack | Release Engineering | `rg -n \"G5-002\" release_evidence/2026-02-12-prod-readiness/gate-5/signoff/g5-006-release-notes-signoff-pack.md` | signoff-pack reference proof | `gate-5/signoff/g5-006-release-notes-signoff-pack.md` |
