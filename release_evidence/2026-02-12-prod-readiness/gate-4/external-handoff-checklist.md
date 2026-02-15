# Gate 4 External Handoff Checklist

Generated UTC: 2026-02-15T16:24:48Z

## G4-001 / G4-002 Terraform Apply + Drift-Free Re-Plan

Owner: Platform + SRE

| Step | Command | Expected Artifact | Destination Path |
|---|---|---|---|
| Confirm staging tfvars values | review `infra/terraform/environments/staging.tfvars` (`project_id`, image refs, issuer/audience, queue/service names) | approved snapshot with reviewer initials | `gate-4/terraform/staging.tfvars.approved.snapshot` |
| Reauth CLI | `gcloud auth login` | successful reauth transcript | `gate-4/terraform/command-logs/gcloud_auth_login_manual.log` |
| Reauth ADC | `gcloud auth application-default login` | ADC ready transcript | `gate-4/terraform/command-logs/gcloud_adc_login_manual.log` |
| Plan | `terraform -chdir=infra/terraform plan -var-file=environments/staging.tfvars -lock=false -input=false -no-color -out=../../release_evidence/2026-02-12-prod-readiness/gate-4/terraform/staging.tfplan` | plan exits `0` and plan file | `gate-4/terraform/command-logs/terraform_plan_g4_external.log` |
| Apply | `terraform -chdir=infra/terraform apply -input=false -auto-approve -no-color ../../release_evidence/2026-02-12-prod-readiness/gate-4/terraform/staging.tfplan` | apply exits `0` | `gate-4/terraform/command-logs/terraform_apply_g4_external.log` |
| Re-plan | `terraform -chdir=infra/terraform plan -var-file=environments/staging.tfvars -lock=false -input=false -no-color` | exits `0` and `No changes` | `gate-4/terraform/command-logs/terraform_replan_g4_external.log` |

## G4-003 Cloud Run Revision + IAM Capture

Owner: Platform + SRE

| Step | Command | Expected Artifact | Destination Path |
|---|---|---|---|
| Reauth for API access | `gcloud auth login && gcloud auth application-default login` | non-interactive reauth failure cleared | `gate-4/cloud-run/command-logs/gcloud_auth_login_manual.log`, `gate-4/cloud-run/command-logs/gcloud_adc_login_manual.log` |
| Set project | `gcloud config set project dataverse-dev-471815` | project set transcript | `gate-4/cloud-run/command-logs/gcloud_project_set_manual.log` |
| List services | `gcloud run services list --project=dataverse-dev-471815 --region=us-central1 --platform=managed --format=json` | JSON list with `arco-api-staging` and `arco-compactor-staging` | `gate-4/cloud-run/command-logs/gcloud_run_services_list_g4_external.json` |
| API describe | `gcloud run services describe arco-api-staging --project=dataverse-dev-471815 --region=us-central1 --platform=managed --format=json` | service + latest revision details | `gate-4/cloud-run/command-logs/gcloud_run_service_describe_api_staging_g4_external.json` |
| Compactor describe | `gcloud run services describe arco-compactor-staging --project=dataverse-dev-471815 --region=us-central1 --platform=managed --format=json` | service + latest revision details | `gate-4/cloud-run/command-logs/gcloud_run_service_describe_compactor_staging_g4_external.json` |
| API revisions | `gcloud run revisions list --project=dataverse-dev-471815 --region=us-central1 --platform=managed --service=arco-api-staging --format=json` | revision history JSON | `gate-4/cloud-run/command-logs/gcloud_run_revisions_api_staging_g4_external.json` |
| Compactor revisions | `gcloud run revisions list --project=dataverse-dev-471815 --region=us-central1 --platform=managed --service=arco-compactor-staging --format=json` | revision history JSON | `gate-4/cloud-run/command-logs/gcloud_run_revisions_compactor_staging_g4_external.json` |
| API IAM policy | `gcloud run services get-iam-policy arco-api-staging --project=dataverse-dev-471815 --region=us-central1 --platform=managed --format=json` | IAM bindings JSON | `gate-4/cloud-run/command-logs/gcloud_run_iam_policy_api_staging_g4_external.json` |
| Compactor IAM policy | `gcloud run services get-iam-policy arco-compactor-staging --project=dataverse-dev-471815 --region=us-central1 --platform=managed --format=json` | IAM bindings JSON | `gate-4/cloud-run/command-logs/gcloud_run_iam_policy_compactor_staging_g4_external.json` |
| Project IAM policy | `gcloud projects get-iam-policy dataverse-dev-471815 --format=json` | project IAM bindings JSON | `gate-4/cloud-run/command-logs/gcloud_project_iam_policy_g4_external.json` |

## G4-004 / G4-005 Staging Observability Verification

Owner: Observability Team

| Step | Command / Action | Expected Artifact | Destination Path |
|---|---|---|---|
| Dashboard visibility | open deployed dashboard and capture active panel screenshot | screenshot + URL | `gate-4/observability/dashboard_staging_screenshot.png`, `gate-4/observability/dashboard_staging_url.txt` |
| Scrape target health | `curl -f https://<staging-observability-endpoint>/api/v1/targets` | `arco-api` and `arco-compactor` healthy | `gate-4/observability/command-logs/staging_scrape_targets.json` |
| Live threshold drill | trigger controlled signal and export alert timeline | fire/ack/resolve timestamps | `gate-4/observability/staging_threshold_drill_timeline.md` |
| Threshold signoff | reviewer confirmation of alert behavior vs runbook thresholds | signed markdown record | `gate-4/observability/staging_threshold_signoff.md` |

## G4-006 Incident Drill + Signoff

Owner: SRE

| Step | Command / Action | Expected Artifact | Destination Path |
|---|---|---|---|
| Execute drill | run staging incident scenario from SRE runbook | timestamped transcript | `gate-4/drills/staging_incident_drill_transcript.md` |
| Export alert timeline | export incident lifecycle from alerting backend | JSON timeline | `gate-4/drills/staging_incident_alert_timeline.json` |
| Reviewer approvals | collect SRE + Platform + Observability signoff | signed decision record | `gate-4/drills/staging_incident_drill_signoff.md` |
