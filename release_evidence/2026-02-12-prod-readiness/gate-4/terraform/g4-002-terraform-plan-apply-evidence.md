# G4-002 Terraform Plan/Apply Evidence

Generated UTC: 2026-02-14T04:51:40Z
Status: PARTIAL (interactive GCP reauthentication required to complete apply + re-plan)

## Executed Commands

Source: `terraform/terraform-command-status.tsv`

| Step | Exit | Notes |
|---|---:|---|
| `terraform init -backend=false` | `0` | Provider init succeeded. |
| `terraform validate` | `0` | Configuration valid. |
| `terraform plan -var-file=environments/staging.tfvars` | `1` | Planned resources were produced, then provider auth failed with `invalid_rapt`. |
| `terraform apply` | `SKIPPED` | Not run because plan exited non-zero. |
| Drift-free re-plan | `SKIPPED` | Not run because apply was not executed. |

## Blocking Error

From `terraform/command-logs/terraform_plan_g4.log`:

- `oauth2: "invalid_grant" "reauth related error (invalid_rapt)"`
- Non-interactive session cannot refresh credentials for Google provider.

## External Completion Steps

Owner: Platform + SRE

1. Reauthenticate user credentials for CLI and ADC.
   - Command:
     - `gcloud auth login`
     - `gcloud auth application-default login`
   - Expected output: successful browser-based auth completion with active account/project.
   - Artifact destination:
     - `terraform/command-logs/gcloud_auth_login_manual.log`
     - `terraform/command-logs/gcloud_adc_login_manual.log`

2. Execute staging apply.
   - Command:
     - `# First replace placeholder values in infra/terraform/environments/staging.tfvars`
     - `terraform -chdir=infra/terraform plan -var-file=environments/staging.tfvars -lock=false -input=false -no-color -out=../../release_evidence/2026-02-12-prod-readiness/gate-4/terraform/staging.tfplan`
     - `terraform -chdir=infra/terraform apply -input=false -auto-approve -no-color ../../release_evidence/2026-02-12-prod-readiness/gate-4/terraform/staging.tfplan`
     - `terraform -chdir=infra/terraform plan -var-file=environments/staging.tfvars -lock=false -input=false -no-color`
   - Expected output:
     - Initial plan exits `0`.
     - Apply exits `0`.
     - Re-plan exits `0` and reports no changes.
   - Artifact destination:
     - `terraform/command-logs/terraform_plan_g4_external.log`
     - `terraform/command-logs/terraform_apply_g4_external.log`
     - `terraform/command-logs/terraform_replan_g4_external.log`
     - `terraform/terraform-command-status.tsv`
