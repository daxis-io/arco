project_id     = "arco-testing-20260320"
project_number = "135245112198"
region         = "us-central1"
environment    = "dev"

# Baseline config for the shared dev/test stack. scripts/deploy.sh overrides
# project selection and service images with explicit -var arguments at runtime.
api_image                = "us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-api:task-token-runid-20260321-1749"
compactor_image          = "us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-compactor:dev-latest"
flow_compactor_image     = "us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-compactor:dev-latest"
flow_dispatcher_image    = "us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-dispatcher:task-token-runid-20260321-1749"
flow_sweeper_image       = "us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-sweeper:task-token-runid-20260321-1749"
flow_timer_ingest_image  = "us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-timer-ingest:dev-latest"
flow_worker_image        = "us-central1-docker.pkg.dev/arco-testing-20260320/arco/arco-flow-worker:worker-debug-headers-20260321-2118"
flow_dispatch_target_url = ""
flow_tenant_id           = "tenant-dev"
flow_workspace_id        = "workspace-dev"
api_code_version         = "dev-latest"

compactor_tenant_id    = "tenant-dev"
compactor_workspace_id = "workspace-dev"

api_min_instances       = 0
api_max_instances       = 1
compactor_min_instances = 0
compactor_max_instances = 1
api_cpu                 = "1"
api_memory              = "512Mi"
compactor_cpu           = "1"
compactor_memory        = "512Mi"

api_public                         = false
api_allow_unauthenticated_internal = true
dev_relaxed_catalog_access         = true
allowed_cors_origins               = "*"
jwt_secret_name                    = ""
jwt_issuer                         = ""
jwt_audience                       = ""
vpc_connector_name                 = ""
task_token_secret                  = "dev-arco-task-token-secret"
task_token_issuer                  = "https://arco.dev/task-token"
task_token_audience                = "arco-worker-callback"
task_token_ttl_seconds             = 2400

flow_dispatch_queue_name = "arco-flow-dispatch-dev"
flow_timer_queue_name    = "arco-flow-timers-dev"
