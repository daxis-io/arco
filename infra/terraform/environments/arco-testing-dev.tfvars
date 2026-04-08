project_id     = "arco-testing-20260320"
project_number = "135245112198"
region         = "us-central1"
environment    = "dev"

# Placeholder images for infra-only targeted applies.
# Real service deployment should replace these with built Arco images.
api_image               = "us-docker.pkg.dev/cloudrun/container/hello"
compactor_image         = "us-docker.pkg.dev/cloudrun/container/hello"
flow_compactor_image    = "us-docker.pkg.dev/cloudrun/container/hello"
flow_dispatcher_image   = ""
flow_sweeper_image      = ""
flow_timer_ingest_image = ""
flow_worker_image       = ""
flow_tenant_id          = ""
flow_workspace_id       = ""
api_code_version        = "bootstrap-infra-only"

compactor_tenant_id    = "tenant-dev"
compactor_workspace_id = "workspace-dev"

api_min_instances             = 0
api_max_instances             = 1
compactor_min_instances       = 0
compactor_max_instances       = 1
background_automation_enabled = false
api_cpu                       = "1"
api_memory                    = "512Mi"
compactor_cpu                 = "1"
compactor_memory              = "512Mi"

api_public           = false
allowed_cors_origins = "*"
jwt_secret_name      = ""
jwt_issuer           = ""
jwt_audience         = ""
vpc_connector_name   = ""

flow_dispatch_queue_name = "arco-flow-dispatch-dev"
flow_timer_queue_name    = "arco-flow-timers-dev"
