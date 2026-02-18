# G5-004 Secrets Rotation Policy Evidence

Generated UTC: 2026-02-15T19:29:45Z
Status: BLOCKED-EXTERNAL
Owner: Security Engineering
Supporting owners: Platform + Release Engineering

## Policy

### Secret Classes and Cadence

| Secret class | Source of truth | Rotation cadence | Owner |
|---|---|---|---|
| API JWT signing secret (`jwt_secret_name` / `ARCO_JWT_SECRET`) | GCP Secret Manager | every 90 days or immediately on incident | Security Engineering |
| CI GCP auth credential (OIDC/WIF) | GitHub Actions OIDC + GCP Workload Identity Federation | automatic short-lived token rotation; quarterly provider/SA binding review | Platform + Release Engineering |
| Metrics shared secret (`ARCO_METRICS_SECRET`) | Secret Manager or secure env injection | every 30 days | SRE |

### Rotation Workflow

1. Create new secret version (or key) and record change ticket.
2. Roll forward consumers in staging, then production.
3. Validate auth flows and metrics/health endpoints.
4. Revoke prior secret version after cutover window.
5. Attach evidence logs and approver initials.

### Verification Workflow

1. Confirm Secret Manager IAM wiring is in place for API service account.
2. Confirm CI path is using OIDC/WIF or a freshly rotated key.
3. Confirm policy requirements in `SECURITY.md` are satisfied.
4. Confirm no secrets are present in evidence logs (redaction hygiene check).

## Fresh Evidence Captured

- Terraform wiring shows Secret Manager integration and accessor IAM role.
- ADR-028 documents OIDC migration and automatic token rotation target.
- CI workflow `iam-smoke` job is migrated to OIDC/WIF variables:
  - `vars.GCP_WORKLOAD_IDENTITY_PROVIDER`
  - `vars.GCP_SERVICE_ACCOUNT`
- Static-key auth path (`credentials_json`, `secrets.GCP_SA_KEY_API`) is absent from `.github/workflows/ci.yml`.

Artifacts:
- `release_evidence/2026-02-12-prod-readiness/gate-5/security/g5-004-command-status.tsv`
- `release_evidence/2026-02-12-prod-readiness/gate-5/security/command-logs/20260215T192525Z_ci_static_key_path_absence_check.log`
- `release_evidence/2026-02-12-prod-readiness/gate-5/security/command-logs/20260215T192525Z_ci_oidc_wif_usage_check.log`
- `release_evidence/2026-02-12-prod-readiness/gate-5/security/command-logs/20260215T192525Z_security_policy_requirements_scan_refresh.log`
- `release_evidence/2026-02-12-prod-readiness/gate-5/security/command-logs/20260215T182351Z_secret_manager_wiring_scan.log`

## Remaining Gap

Signal cannot be marked GO until Security Engineering signs the policy and repository/environment variables are confirmed in GitHub with a successful `iam-smoke` run using OIDC/WIF.

## External Handoff Steps

| Step | Owner | Command/Action | Expected artifact | Destination |
|---|---|---|---|---|
| Configure GitHub variables | Platform + Release Engineering | set `GCP_WORKLOAD_IDENTITY_PROVIDER` and `GCP_SERVICE_ACCOUNT` in repo/environment variables | settings screenshot or export (redacted) | `gate-5/security/ci_oidc_variable_setup.md` |
| Validate OIDC path in CI run | Platform + Release Engineering | trigger `iam-smoke` on `main` and capture successful auth/test logs | successful workflow run transcript | `gate-5/security/ci_oidc_smoke_run.log` |
| Security approval | Security Engineering | sign this policy artifact with approver + UTC timestamp | signed approval record | `gate-5/security/g5-004-secrets-rotation-policy-evidence.md` |
