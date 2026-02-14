# Staging TFVars + Locked IAM/SA Readiness Proof (G4-001)

Generated UTC: 2026-02-14T04:51:14Z
Sources:
- `infra/terraform/environments/staging.tfvars`
- `infra/terraform/variables.tf`
- `infra/terraform/iam.tf`
- `infra/terraform/iam_conditions.tf`
- `infra/terraform/cloud_tasks.tf`

## Required Variable Coverage
- Required variables from `variables.tf`: project_id, api_image, compactor_image, compactor_tenant_id, compactor_workspace_id
- Keys present in `staging.tfvars`: project_id, region, environment, api_image, compactor_image, api_code_version, compactor_tenant_id, compactor_workspace_id, anti_entropy_domain, anti_entropy_schedule, api_min_instances, api_max_instances, compactor_min_instances, compactor_max_instances, api_cpu, api_memory, compactor_cpu, compactor_memory, api_public, allowed_cors_origins, jwt_secret_name, jwt_issuer, jwt_audience, vpc_connector_name, flow_dispatch_queue_name, flow_timer_queue_name, flow_dispatcher_service_name
- Missing required keys: none

## Locked Service Account Model Checks
| Check | Result |
|---|---|
| `api_service_account` | PASS |
| `compactor_service_account` | PASS |
| `compactor_antientropy_service_account` | PASS |
| `invoker_service_account` | PASS |
| `api_cloud_tasks_enqueuer_binding` | PASS |
| `api_act_as_invoker_binding` | PASS |
| `cloud_tasks_impersonate_invoker_binding` | PASS |
| `no_contains_in_live_iam_conditions` | PASS |
| `anchored_regex_in_iam_conditions` | PASS |

## Result
- PASS (local/static): staging tfvars includes all required inputs and IAM/SA model is explicitly locked in Terraform config.
- NOTE: live environment closure still requires authenticated plan/apply + reviewer signoff.
