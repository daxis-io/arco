# Deployment Evidence Blocker

Date: 2025-12-31
Status: DEFERRED (alpha/local-only; no deployment yet)

## What's Present (IaC Artifacts)
- `/Users/ethanurbanski/arco/infra/terraform/main.tf` - Main Terraform configuration
- `/Users/ethanurbanski/arco/infra/terraform/cloud_run.tf` - Cloud Run service definitions
- `/Users/ethanurbanski/arco/infra/terraform/cloud_run_job.tf` - Cloud Run job definitions
- `/Users/ethanurbanski/arco/infra/terraform/iam.tf` - IAM bindings
- `/Users/ethanurbanski/arco/infra/terraform/variables.tf` - Variable definitions
- `/Users/ethanurbanski/arco/infra/terraform/environments/dev.tfvars.example` - Example tfvars
- `/Users/ethanurbanski/arco/scripts/deploy.sh` - Deployment script
- `/Users/ethanurbanski/arco/scripts/rollback.sh` - Rollback script

## What's Validated
- `terraform init` - PASSED (provider installed)
- `terraform validate` - PASSED (configuration valid)

## What's Missing (Proof Artifacts)
- `terraform plan` output with real variable values
- `terraform apply` output showing successful deployment
- Cloud Run service listings (`gcloud run services list`) — current `gcloud-run-services.json` contains reauth error
- Cloud Run revision details (`gcloud run revisions list`) — current `gcloud-run-revisions-*.json` contain reauth errors
- IAM binding verification

## Why Deferred
- No deployment yet (alpha/local-only)
- Placeholder tenant/workspace IDs acceptable until first deployment
- gcloud auth not required until Cloud Run exists

## Required Actions (when ready to deploy)
1. Provide values for `compactor_tenant_id` and `compactor_workspace_id`
2. Run `gcloud auth login` interactively to refresh credentials
3. Run `terraform plan -var-file=environments/dev.tfvars` with complete variable set
4. Capture Cloud Run service/revision listings
5. Store artifacts in `release_evidence/2025-12-30-catalog-mvp/deploy/`

## Acceptable Evidence (per industry standards)
- `terraform plan` output showing planned resources
- `terraform apply` output or state file excerpt showing deployed resources
- Cloud Run service list with revision status
- IAM policy export showing service account bindings

## Variable Requirements

| Variable | Description | Source |
|----------|-------------|--------|
| `compactor_tenant_id` | Tenant ID for compactor jobs | User-provided |
| `compactor_workspace_id` | Workspace ID for compactor jobs | User-provided |
| `project_id` | GCP project ID | From `gcloud config list` |
| `api_image` | API container image | Registry path |
| `compactor_image` | Compactor container image | Registry path |
