# G4-003 Cloud Run Revision + IAM Evidence

Generated UTC: 2026-02-14T04:51:40Z
Status: BLOCKED-EXTERNAL (interactive GCP reauthentication required)

## Attempted Commands

Source: `cloud-run/cloud-run-command-status.tsv`

- `gcloud run services list --project=dataverse-dev-471815 --region=us-central1 --format=json` -> exit `1`
- `gcloud run services describe arco-api-staging ...` -> exit `1`
- `gcloud run services describe arco-compactor-staging ...` -> exit `1`
- `gcloud run revisions list --service=arco-api-staging ...` -> exit `1`
- `gcloud run revisions list --service=arco-compactor-staging ...` -> exit `1`
- `gcloud run services get-iam-policy arco-api-staging ...` -> exit `1`
- `gcloud run services get-iam-policy arco-compactor-staging ...` -> exit `1`
- `gcloud projects get-iam-policy dataverse-dev-471815 --format=json` -> exit `1`

All commands failed with non-interactive reauth errors:
- `Reauthentication failed. cannot prompt during non-interactive execution.`

## External Completion Steps

Owner: Platform + SRE

1. Refresh CLI credentials.
   - Command:
     - `gcloud auth login`
     - `gcloud config set project dataverse-dev-471815`
   - Expected output: active account and project set without reauth error.
   - Artifact destination:
     - `cloud-run/command-logs/gcloud_auth_login_manual.log`
     - `cloud-run/command-logs/gcloud_project_set_manual.log`

2. Capture service and revision evidence.
   - Command:
     - `gcloud run services list --project=dataverse-dev-471815 --region=us-central1 --platform=managed --format=json > release_evidence/2026-02-12-prod-readiness/gate-4/cloud-run/command-logs/gcloud_run_services_list_g4_external.json`
     - `gcloud run services describe arco-api-staging --project=dataverse-dev-471815 --region=us-central1 --platform=managed --format=json > release_evidence/2026-02-12-prod-readiness/gate-4/cloud-run/command-logs/gcloud_run_service_describe_api_staging_g4_external.json`
     - `gcloud run services describe arco-compactor-staging --project=dataverse-dev-471815 --region=us-central1 --platform=managed --format=json > release_evidence/2026-02-12-prod-readiness/gate-4/cloud-run/command-logs/gcloud_run_service_describe_compactor_staging_g4_external.json`
     - `gcloud run revisions list --project=dataverse-dev-471815 --region=us-central1 --platform=managed --service=arco-api-staging --format=json > release_evidence/2026-02-12-prod-readiness/gate-4/cloud-run/command-logs/gcloud_run_revisions_api_staging_g4_external.json`
     - `gcloud run revisions list --project=dataverse-dev-471815 --region=us-central1 --platform=managed --service=arco-compactor-staging --format=json > release_evidence/2026-02-12-prod-readiness/gate-4/cloud-run/command-logs/gcloud_run_revisions_compactor_staging_g4_external.json`
   - Expected output: JSON payloads with current service revision names, traffic targets, and ready status.

3. Capture IAM policy evidence.
   - Command:
     - `gcloud run services get-iam-policy arco-api-staging --project=dataverse-dev-471815 --region=us-central1 --platform=managed --format=json > release_evidence/2026-02-12-prod-readiness/gate-4/cloud-run/command-logs/gcloud_run_iam_policy_api_staging_g4_external.json`
     - `gcloud run services get-iam-policy arco-compactor-staging --project=dataverse-dev-471815 --region=us-central1 --platform=managed --format=json > release_evidence/2026-02-12-prod-readiness/gate-4/cloud-run/command-logs/gcloud_run_iam_policy_compactor_staging_g4_external.json`
   - Expected output: IAM bindings showing required `roles/run.invoker` and service account principals.
