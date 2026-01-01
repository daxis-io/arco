# Release Evidence Index — 2025-12-30-audit

This pack contains Arco-only evidence artifacts and references.

## Build & Test Logs
- cargo fmt --check: `release_evidence/2025-12-30-audit/ci-logs/cargo-fmt.txt` (PASS)
- cargo clippy -- -D warnings: `release_evidence/2025-12-30-audit/ci-logs/cargo-clippy.txt` (PASS)
- cargo test --workspace --all-features: `release_evidence/2025-12-30-audit/ci-logs/cargo-test.txt` (PASS)
- cargo doc (RUSTDOCFLAGS=-D warnings): `release_evidence/2025-12-30-audit/ci-logs/cargo-doc.txt` (PASS)
- cargo deny check: `release_evidence/2025-12-30-audit/ci-logs/cargo-deny.txt` (PASS)
- buf lint (--path proto): `release_evidence/2025-12-30-audit/ci-logs/buf-lint.txt` (PASS)
- buf breaking (--path proto): `release_evidence/2025-12-30-audit/ci-logs/buf-breaking.txt` (PASS)

## Deployment Evidence (references)
- Terraform root: `infra/terraform/main.tf`
- Cloud Run services: `infra/terraform/cloud_run.tf`
- Cloud Run jobs + scheduler: `infra/terraform/cloud_run_job.tf`
- IAM bindings: `infra/terraform/iam.tf`
- Deploy script (compactor-first): `scripts/deploy.sh`
- Rollback script: `scripts/rollback.sh`

## Observability Evidence (references)
- Alert rules: `infra/monitoring/alerts.yaml`
- Dashboard JSON: `infra/monitoring/dashboard.json`
- Metrics catalog: `docs/runbooks/metrics-catalog.md`
- Runbook-shipped dashboard: `docs/runbooks/grafana-dashboard.json`
- Runbook-shipped alerts: `docs/runbooks/prometheus-alerts.yaml`

## Runbooks Evidence (references)
- Storage integrity verification: `docs/runbooks/storage-integrity-verification.md`
- GC failure handling: `docs/runbooks/gc-failure.md`
- Task stuck dispatched: `docs/runbooks/task-stuck-dispatched.md`
- High task failure rate: `docs/runbooks/high-task-failure-rate.md`
- Scheduler not progressing: `docs/runbooks/scheduler-not-progressing.md`

## Security Evidence (references)
- Security policy: `SECURITY.md`
- Dependency policy: `deny.toml`
- Security audit workflow: `.github/workflows/security-audit.yml`
- SBOM workflow: `.github/workflows/release-sbom.yml`
