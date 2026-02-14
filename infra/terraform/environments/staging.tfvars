# Arco Staging Environment Configuration
#
# This file is committed as a complete, non-secret baseline for Gate 4 evidence.
# Replace project/image/identity values during external staging execution if needed.

project_id  = "replace-with-staging-project-id"
region      = "us-central1"
environment = "staging"

# Container images
api_image        = "us-central1-docker.pkg.dev/replace-with-staging-project/arco/arco-api:staging"
compactor_image  = "us-central1-docker.pkg.dev/replace-with-staging-project/arco/arco-compactor:staging"
api_code_version = "2026-02-12-prod-readiness"

# Compactor anti-entropy scope (required)
compactor_tenant_id    = "staging"
compactor_workspace_id = "staging"
anti_entropy_domain    = "catalog"
anti_entropy_schedule  = "*/15 * * * *"

# Cloud Run scaling and resources
api_min_instances       = 1
api_max_instances       = 5
compactor_min_instances = 1
compactor_max_instances = 2

api_cpu         = "2"
api_memory      = "1Gi"
compactor_cpu   = "2"
compactor_memory = "2Gi"

# Security and networking (staging defaults to private)
api_public            = false
allowed_cors_origins  = "https://staging.arco.example.com"
jwt_secret_name       = "arco-jwt-secret-staging"
jwt_issuer            = "https://issuer.example.com/staging"
jwt_audience          = "arco-staging"
vpc_connector_name    = ""

# Flow dispatcher Cloud Tasks queues
flow_dispatch_queue_name    = "arco-flow-dispatch-staging"
flow_timer_queue_name       = "arco-flow-timers-staging"
flow_dispatcher_service_name = "arco-flow-dispatcher-staging"
