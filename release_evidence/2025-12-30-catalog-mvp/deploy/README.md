# Deployment Evidence

Date: 2025-12-31
Status: Partially complete - awaiting variable values and gcloud reauth

## Contents

### Completed Evidence
| Artifact | Status |
|----------|--------|
| `terraform-version.txt` | Captured (v1.12.2) |
| `terraform-init.txt` | PASSED |
| `terraform-validate.txt` | PASSED |
| `gcloud-version.txt` | Captured (542.0.0) |
| `gcloud-auth-list.txt` | Captured |
| `gcloud-config-list.txt` | Captured |
| `blocker-deploy-evidence.md` | Documents blockers |

### Pending Evidence
| Artifact | Blocker |
|----------|---------|
| `terraform-plan.txt` | Needs `compactor_tenant_id` and `compactor_workspace_id` |
| `gcloud-run-services-complete.json` | Needs `gcloud auth login` (interactive); existing `gcloud-run-services.json` contains reauth error |
| `gcloud-run-revisions-api-complete.json` | Needs `gcloud auth login` (interactive); existing `gcloud-run-revisions-api.json` contains reauth error |
| `gcloud-run-revisions-compactor-complete.json` | Needs `gcloud auth login` (interactive); existing `gcloud-run-revisions-compactor.json` contains reauth error |

## How to Complete Terraform Evidence

1. Create a tfvars file with required variables:
   ```hcl
   # Copy from environments/dev.tfvars.example and add:
   compactor_tenant_id   = "your-tenant-id"
   compactor_workspace_id = "your-workspace-id"
   ```

2. Run terraform plan:
   ```bash
   cd infra/terraform
   terraform plan -var-file=environments/dev.tfvars > ../../release_evidence/2025-12-30-catalog-mvp/deploy/terraform-plan-complete.txt 2>&1
   ```

3. Update `blocker-deploy-evidence.md` to mark terraform plan complete

## How to Complete gcloud Evidence

1. Authenticate interactively:
   ```bash
   gcloud auth login
   ```

2. Capture Cloud Run service listings:
   ```bash
   cd /Users/ethanurbanski/arco/release_evidence/2025-12-30-catalog-mvp/deploy
   gcloud run services list --format=json > gcloud-run-services-complete.json
   gcloud run revisions list --service=arco-api --format=json > gcloud-run-revisions-api-complete.json
   gcloud run revisions list --service=arco-compactor --format=json > gcloud-run-revisions-compactor-complete.json
   ```

3. Update `blocker-deploy-evidence.md` to mark gcloud captures complete

## Required Variable Values

| Variable | Description | Source |
|----------|-------------|--------|
| `compactor_tenant_id` | Tenant ID for compactor jobs | Workspace configuration |
| `compactor_workspace_id` | Workspace ID for compactor jobs | Workspace configuration |

## IaC Artifacts Present

| Artifact | Path | Status |
|----------|------|--------|
| Main TF Config | `infra/terraform/main.tf` | Present |
| Cloud Run Services | `infra/terraform/cloud_run.tf` | Present |
| Cloud Run Jobs | `infra/terraform/cloud_run_job.tf` | Present |
| IAM Bindings | `infra/terraform/iam.tf` | Present |
| IAM Conditions | `infra/terraform/iam_conditions.tf` | Present |
| Variables | `infra/terraform/variables.tf` | Present |
| Deploy Script | `scripts/deploy.sh` | Present |
| Rollback Script | `scripts/rollback.sh` | Present |
