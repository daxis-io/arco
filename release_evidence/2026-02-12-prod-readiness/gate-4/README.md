# Gate 4 Evidence

Batch 3 Gate 4 execution artifacts (2026-02-14 UTC refresh).

## Terraform

- `terraform/staging_tfvars_iam_readiness_proof.md`
- `terraform/g4-001-readiness-status.md`
- `terraform/terraform-command-status.tsv`
- `terraform/command-logs/terraform_init_g4.log`
- `terraform/command-logs/terraform_validate_g4.log`
- `terraform/command-logs/terraform_plan_g4.log`
- `terraform/command-logs/terraform_apply_g4.log`
- `terraform/command-logs/terraform_replan_g4.log`
- `terraform/g4-002-terraform-plan-apply-evidence.md`

## Cloud Run + IAM

- `cloud-run/cloud-run-command-status.tsv`
- `cloud-run/command-logs/gcloud_run_services_list_g4.log`
- `cloud-run/command-logs/gcloud_run_service_describe_api_staging_g4.log`
- `cloud-run/command-logs/gcloud_run_service_describe_compactor_staging_g4.log`
- `cloud-run/command-logs/gcloud_run_revisions_api_staging_g4.log`
- `cloud-run/command-logs/gcloud_run_revisions_compactor_staging_g4.log`
- `cloud-run/command-logs/gcloud_run_iam_policy_api_staging_g4.log`
- `cloud-run/command-logs/gcloud_run_iam_policy_compactor_staging_g4.log`
- `cloud-run/command-logs/gcloud_project_iam_policy_g4.log`
- `cloud-run/g4-003-cloud-run-iam-evidence.md`

## Observability

- `observability/observability-command-status.tsv`
- `observability/observability_dashboard_config_proof.md`
- `observability/observability_scrape_wiring_proof.md`
- `observability/observability_alert_threshold_proof.md`
- `observability/observability_gate4_alert_drill.test.yaml`
- `observability/command-logs/promtool_test_g4_alert_drill.log`
- `observability/g4-004-observability-deployment-proof.md`
- `observability/g4-005-slo-burn-rate-thresholds-proof.md`

## Drills

- `drills/g4-006-incident-drill-signoff-handoff.md`

## Cross-Signal Handoff

- `external-handoff-checklist.md`
