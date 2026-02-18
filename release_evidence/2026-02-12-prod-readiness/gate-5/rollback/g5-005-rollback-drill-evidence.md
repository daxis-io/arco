# G5-005 Rollback Drill Evidence

Generated UTC: 2026-02-15T19:29:45Z
Status: BLOCKED-EXTERNAL
Owner: SRE

## Closure Requirement

Execute rollback drill and verify post-rollback service health + integrity invariants.

## Fresh Evidence Captured (Local)

1. Rollback runbook + integrity runbook presence checks passed.
2. Rollback script syntax check passed.
3. Refresh dry-run rollback attempt failed due non-interactive `gcloud` reauthentication requirement.
4. Integrity verifier dry-run passed (storage checks skipped without tenant/workspace/bucket inputs).

## Why This Signal Is External

Live rollback drill requires authenticated Cloud Run/GCP access and a staged release target with real tenant/workspace integrity checks.

## Evidence Artifacts

- `release_evidence/2026-02-12-prod-readiness/gate-5/rollback/g5-005-command-status.tsv`
- `release_evidence/2026-02-12-prod-readiness/gate-5/rollback/command-logs/20260215T192701Z_rollback_script_dry_run_attempt_refresh.log`
- `release_evidence/2026-02-12-prod-readiness/gate-5/rollback/command-logs/20260215T192703Z_integrity_verifier_dry_run_refresh.log`

## External Handoff Steps

| Step | Owner | Command | Expected artifact | Destination |
|---|---|---|---|---|
| Reauthenticate gcloud | SRE | `gcloud auth login && gcloud auth application-default login` | auth transcript | `gate-5/rollback/command-logs/gcloud_auth_login_manual.log` |
| Execute staged rollback drill | SRE | `PROJECT_ID=<project> ./scripts/rollback.sh --env staging --include-compactor` | rollback transcript | `gate-5/rollback/command-logs/rollback_staging_run.log` |
| Verify API + compactor health | SRE | `curl -f <api-health-url>` and `curl -f <compactor-health-url>` | health check output | `gate-5/rollback/post_rollback_health_checks.log` |
| Verify storage integrity | SRE | `ARCO_STORAGE_BUCKET=<bucket> cargo xtask verify-integrity --tenant=<tenant> --workspace=<workspace>` | integrity report | `gate-5/rollback/post_rollback_integrity_report.log` |
| Compare pre/post manifest state | SRE | capture manifest hashes before and after rollback | hash comparison report | `gate-5/rollback/manifest_hash_comparison.md` |
