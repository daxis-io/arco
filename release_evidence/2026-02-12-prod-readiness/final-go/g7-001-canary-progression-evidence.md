# G7-001 Canary Progression Evidence (5% -> 25% -> 100%)

Generated UTC: 2026-02-15T19:29:45Z
Status: BLOCKED-EXTERNAL
Owner: Release Engineering

## Closure Requirement

Execute production canary progression with health gates at 5%, 25%, and 100% traffic.

## Fresh Evidence Captured (Local)

1. Deployment script syntax is valid.
2. `gcloud auth print-access-token` fails in this non-interactive session (reauth required).
3. Required production canary environment variables are not available in this workspace (`PROD_PROJECT_ID`, `CANARY_SERVICE`, `NEW_REVISION`, `OLD_REVISION`).

## Why This Signal Is External

Canary progression requires production Cloud Run traffic changes, live health telemetry, and change-approval execution.

## Evidence Artifacts

- `release_evidence/2026-02-12-prod-readiness/final-go/g7-001-command-status.tsv`
- `release_evidence/2026-02-12-prod-readiness/final-go/command-logs/20260215T192809Z_deploy_script_syntax_refresh.log`
- `release_evidence/2026-02-12-prod-readiness/final-go/command-logs/20260215T192809Z_gcloud_access_token_preflight.log`
- `release_evidence/2026-02-12-prod-readiness/final-go/command-logs/20260215T192811Z_canary_env_requirements_check.log`

## External Handoff Steps

| Step | Owner | Command | Expected artifact | Destination |
|---|---|---|---|---|
| Capture baseline health | Release Engineering + SRE | `curl -f <prod-health-endpoint>` and export key SLO queries | pre-canary health snapshot | `final-go/canary_baseline_health.json` |
| Shift to 5% canary | Release Engineering | `gcloud run services update-traffic arco-api-prod --region=us-central1 --project=<project> --to-revisions=<new_rev>=5,<old_rev>=95` | traffic change transcript | `final-go/canary_5pct_update.log` |
| Evaluate 5% gate | SRE | run latency/error/backlog gate queries for hold window | gate decision record | `final-go/canary_5pct_gate.md` |
| Shift to 25% canary | Release Engineering | `gcloud run services update-traffic ... <new_rev>=25,<old_rev>=75` | traffic change transcript | `final-go/canary_25pct_update.log` |
| Evaluate 25% gate | SRE | run same health gate queries | gate decision record | `final-go/canary_25pct_gate.md` |
| Shift to 100% | Release Engineering | `gcloud run services update-traffic ... <new_rev>=100` | promotion transcript | `final-go/canary_100pct_update.log` |
| Final health validation | SRE | run post-promotion SLO checks | final canary report | `final-go/canary_final_health_report.md` |
