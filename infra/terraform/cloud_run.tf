# Arco Cloud Run Services
#
# Deploys API and Compactor as Cloud Run services.
#
# ## Deployment Strategy
#
# CRITICAL: Compactor must be healthy BEFORE API accepts traffic.
# The deploy script enforces this ordering (see scripts/deploy.sh).
#
# ## Service Architecture
#
# - API: Handles HTTP/gRPC requests, authenticates users, routes to catalog
# - Compactor: Background worker that compacts ledger events to Parquet snapshots

# ============================================================================
# API Service
# ============================================================================

resource "google_cloud_run_v2_service" "api" {
  name     = "arco-api-${var.environment}"
  location = var.region
  project  = var.project_id

  # Don't route traffic until manually verified (compactor-first deployment)
  ingress = var.api_public ? "INGRESS_TRAFFIC_ALL" : "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.api.email

    scaling {
      min_instance_count = var.api_min_instances
      max_instance_count = var.api_max_instances
    }

    # VPC connector for internal services
    dynamic "vpc_access" {
      for_each = var.vpc_connector_name != "" ? [1] : []
      content {
        connector = var.vpc_connector_name
        egress    = "PRIVATE_RANGES_ONLY"
      }
    }

    containers {
      image = var.api_image

      resources {
        limits = {
          cpu    = var.api_cpu
          memory = var.api_memory
        }
        cpu_idle          = true # Scale to zero when idle
        startup_cpu_boost = true # Faster cold starts
      }

      # Health checks
      startup_probe {
        http_get {
          path = "/ready"
          port = 8080
        }
        initial_delay_seconds = 2
        timeout_seconds       = 5
        period_seconds        = 5
        failure_threshold     = 3
      }

      liveness_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        timeout_seconds   = 5
        period_seconds    = 30
        failure_threshold = 3
      }

      ports {
        container_port = 8080
        name           = "http1"
      }

      # Environment variables
      env {
        name  = "ARCO_HTTP_PORT"
        value = "8080"
      }

      env {
        name  = "ARCO_GRPC_PORT"
        value = "9090"
      }

      env {
        name  = "ARCO_DEBUG"
        value = var.environment == "dev" && !var.api_public ? "true" : "false"
      }

      env {
        name  = "ARCO_CORS_ALLOWED_ORIGINS"
        value = var.allowed_cors_origins
      }

      env {
        name  = "ARCO_JWT_ISSUER"
        value = var.jwt_issuer
      }

      env {
        name  = "ARCO_JWT_AUDIENCE"
        value = var.jwt_audience
      }

      env {
        name  = "ARCO_STORAGE_BUCKET"
        value = google_storage_bucket.catalog.name
      }

      env {
        name  = "ARCO_ORCH_COMPACTOR_URL"
        value = google_cloud_run_v2_service.flow_compactor.uri
      }

      env {
        name  = "ARCO_ENVIRONMENT"
        value = var.environment
      }

      env {
        name  = "ARCO_API_PUBLIC"
        value = var.api_public ? "true" : "false"
      }

      env {
        name  = "ARCO_CODE_VERSION"
        value = var.api_code_version
      }

      # JWT secret from Secret Manager (if configured)
      dynamic "env" {
        for_each = var.jwt_secret_name != "" ? [1] : []
        content {
          name = "ARCO_JWT_SECRET"
          value_source {
            secret_key_ref {
              secret  = var.jwt_secret_name
              version = "latest"
            }
          }
        }
      }
    }
  }

  # Traffic routing (can be customized for canary deployments)
  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  lifecycle {
    ignore_changes = [
      # Ignore client-side changes to these fields
      client,
      client_version,
    ]
  }
}

# ============================================================================
# Compactor Service
# ============================================================================

resource "google_cloud_run_v2_service" "compactor" {
  name     = "arco-compactor-${var.environment}"
  location = var.region
  project  = var.project_id

  # Compactor is always internal-only
  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.compactor.email

    scaling {
      # Compactor runs continuously (not scaled to zero)
      min_instance_count = var.compactor_min_instances
      max_instance_count = var.compactor_max_instances
    }

    # VPC connector for internal services
    dynamic "vpc_access" {
      for_each = var.vpc_connector_name != "" ? [1] : []
      content {
        connector = var.vpc_connector_name
        egress    = "PRIVATE_RANGES_ONLY"
      }
    }

    containers {
      image = var.compactor_image

      resources {
        limits = {
          cpu    = var.compactor_cpu
          memory = var.compactor_memory
        }
        cpu_idle          = false # Always allocated (background worker)
        startup_cpu_boost = true
      }

      # Health checks
      startup_probe {
        http_get {
          path = "/health"
          port = 8081
        }
        initial_delay_seconds = 5
        timeout_seconds       = 10
        period_seconds        = 10
        failure_threshold     = 6
      }

      liveness_probe {
        http_get {
          path = "/health"
          port = 8081
        }
        timeout_seconds   = 10
        period_seconds    = 60
        failure_threshold = 3
      }

      ports {
        container_port = 8081
        name           = "http1"
      }

      # Environment variables
      env {
        name  = "ARCO_COMPACTOR_PORT"
        value = "8081"
      }

      env {
        name  = "ARCO_STORAGE_BUCKET"
        value = google_storage_bucket.catalog.name
      }

      env {
        name  = "ARCO_ENVIRONMENT"
        value = var.environment
      }

      # Compaction interval (seconds)
      env {
        name  = "ARCO_COMPACTOR_INTERVAL_SECS"
        value = var.environment == "prod" ? "60" : "30"
      }

      # GC settings
      env {
        name  = "ARCO_COMPACTOR_GC_ENABLED"
        value = "true"
      }

      env {
        name  = "ARCO_COMPACTOR_GC_RETENTION_DAYS"
        value = "7"
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  lifecycle {
    ignore_changes = [
      client,
      client_version,
    ]
  }
}

# ============================================================================
# Arco Flow Orchestration Compactor Service
# ============================================================================

resource "google_cloud_run_v2_service" "flow_compactor" {
  name     = "arco-flow-compactor-${var.environment}"
  location = var.region
  project  = var.project_id

  # Internal-only service (invoked by other flow services)
  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.compactor.email

    scaling {
      # Keep warm for low-latency compaction in pipeline tests.
      min_instance_count = 1
      max_instance_count = 1
    }

    dynamic "vpc_access" {
      for_each = var.vpc_connector_name != "" ? [1] : []
      content {
        connector = var.vpc_connector_name
        egress    = "PRIVATE_RANGES_ONLY"
      }
    }

    containers {
      image = var.flow_compactor_image

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = false
        startup_cpu_boost = true
      }

      startup_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        initial_delay_seconds = 2
        timeout_seconds       = 5
        period_seconds        = 5
        failure_threshold     = 6
      }

      liveness_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        timeout_seconds   = 5
        period_seconds    = 30
        failure_threshold = 3
      }

      ports {
        container_port = 8080
        name           = "http1"
      }

      env {
        name  = "ARCO_TENANT_ID"
        value = var.compactor_tenant_id
      }

      env {
        name  = "ARCO_WORKSPACE_ID"
        value = var.compactor_workspace_id
      }

      env {
        name  = "ARCO_STORAGE_BUCKET"
        value = google_storage_bucket.catalog.name
      }

      env {
        name  = "ARCO_REQUIRE_TENANT_SECRET"
        value = var.environment == "dev" ? "false" : "true"
      }

      dynamic "env" {
        for_each = length(google_secret_manager_secret.tenant_secret) > 0 ? [1] : []
        content {
          name = "ARCO_TENANT_SECRET_B64"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.tenant_secret[0].secret_id
              version = "latest"
            }
          }
        }
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  lifecycle {
    ignore_changes = [
      client,
      client_version,
    ]
  }
}

# ============================================================================
# Arco Flow Dispatcher Service
# ============================================================================

resource "google_cloud_run_v2_service" "flow_dispatcher" {
  name     = "arco-flow-dispatcher-${var.environment}"
  location = var.region
  project  = var.project_id

  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.flow_controller.email

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    dynamic "vpc_access" {
      for_each = var.vpc_connector_name != "" ? [1] : []
      content {
        connector = var.vpc_connector_name
        egress    = "PRIVATE_RANGES_ONLY"
      }
    }

    containers {
      image = var.flow_dispatcher_image

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = false
        startup_cpu_boost = true
      }

      startup_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        initial_delay_seconds = 2
        timeout_seconds       = 5
        period_seconds        = 5
        failure_threshold     = 6
      }

      liveness_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        timeout_seconds   = 5
        period_seconds    = 30
        failure_threshold = 3
      }

      ports {
        container_port = 8080
        name           = "http1"
      }

      env {
        name  = "ARCO_TENANT_ID"
        value = var.compactor_tenant_id
      }

      env {
        name  = "ARCO_WORKSPACE_ID"
        value = var.compactor_workspace_id
      }

      env {
        name  = "ARCO_STORAGE_BUCKET"
        value = google_storage_bucket.catalog.name
      }

      env {
        name  = "ARCO_GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "ARCO_GCP_LOCATION"
        value = var.region
      }

      env {
        name  = "ARCO_FLOW_DISPATCH_TARGET_URL"
        value = "${google_cloud_run_v2_service.flow_worker.uri}/dispatch"
      }

      env {
        name  = "ARCO_FLOW_QUEUE"
        value = "arco-flow-dispatch"
      }

      env {
        name  = "ARCO_FLOW_TIMER_QUEUE"
        value = "arco-flow-timer"
      }

      env {
        name = "ARCO_FLOW_TIMER_TARGET_URL"
        # Use Cloud Run deterministic URLs to avoid a Terraform self-reference cycle.
        # See: https://cloud.google.com/run/docs/routing/ingress#deterministic_url
        value = "https://arco-flow-dispatcher-${var.environment}-${local.project_number}.${var.region}.run.app/timer"
      }

      env {
        name  = "ARCO_FLOW_SERVICE_ACCOUNT_EMAIL"
        value = google_service_account.flow_task_invoker.email
      }

      env {
        name  = "ARCO_ORCH_COMPACTOR_URL"
        value = google_cloud_run_v2_service.flow_compactor.uri
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  lifecycle {
    ignore_changes = [
      client,
      client_version,
    ]
  }
}

# ============================================================================
# Arco Flow Sweeper Service
# ============================================================================

resource "google_cloud_run_v2_service" "flow_sweeper" {
  name     = "arco-flow-sweeper-${var.environment}"
  location = var.region
  project  = var.project_id

  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.flow_controller.email

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    dynamic "vpc_access" {
      for_each = var.vpc_connector_name != "" ? [1] : []
      content {
        connector = var.vpc_connector_name
        egress    = "PRIVATE_RANGES_ONLY"
      }
    }

    containers {
      image = var.flow_sweeper_image

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = false
        startup_cpu_boost = true
      }

      startup_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        initial_delay_seconds = 2
        timeout_seconds       = 5
        period_seconds        = 5
        failure_threshold     = 6
      }

      liveness_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        timeout_seconds   = 5
        period_seconds    = 30
        failure_threshold = 3
      }

      ports {
        container_port = 8080
        name           = "http1"
      }

      env {
        name  = "ARCO_TENANT_ID"
        value = var.compactor_tenant_id
      }

      env {
        name  = "ARCO_WORKSPACE_ID"
        value = var.compactor_workspace_id
      }

      env {
        name  = "ARCO_STORAGE_BUCKET"
        value = google_storage_bucket.catalog.name
      }

      env {
        name  = "ARCO_GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "ARCO_GCP_LOCATION"
        value = var.region
      }

      env {
        name  = "ARCO_FLOW_DISPATCH_TARGET_URL"
        value = "${google_cloud_run_v2_service.flow_worker.uri}/dispatch"
      }

      env {
        name  = "ARCO_FLOW_QUEUE"
        value = "arco-flow-dispatch"
      }

      env {
        name  = "ARCO_FLOW_SERVICE_ACCOUNT_EMAIL"
        value = google_service_account.flow_task_invoker.email
      }

      env {
        name  = "ARCO_ORCH_COMPACTOR_URL"
        value = google_cloud_run_v2_service.flow_compactor.uri
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  lifecycle {
    ignore_changes = [
      client,
      client_version,
    ]
  }
}

# ============================================================================
# Arco Flow Worker Service
# ============================================================================

resource "google_cloud_run_v2_service" "flow_worker" {
  name     = "arco-flow-worker-${var.environment}"
  location = var.region
  project  = var.project_id

  ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.flow_worker.email

    scaling {
      min_instance_count = 0
      max_instance_count = 10
    }

    dynamic "vpc_access" {
      for_each = var.vpc_connector_name != "" ? [1] : []
      content {
        connector = var.vpc_connector_name
        egress    = "PRIVATE_RANGES_ONLY"
      }
    }

    containers {
      image = var.flow_worker_image

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = true
        startup_cpu_boost = true
      }

      startup_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        initial_delay_seconds = 2
        timeout_seconds       = 5
        period_seconds        = 5
        failure_threshold     = 6
      }

      liveness_probe {
        http_get {
          path = "/health"
          port = 8080
        }
        timeout_seconds   = 5
        period_seconds    = 30
        failure_threshold = 3
      }

      ports {
        container_port = 8080
        name           = "http1"
      }

      env {
        name  = "ARCO_FLOW_API_URL"
        value = google_cloud_run_v2_service.api.uri
      }

      env {
        name  = "ARCO_FLOW_TENANT_ID"
        value = var.compactor_tenant_id
      }

      env {
        name  = "ARCO_FLOW_WORKSPACE_ID"
        value = var.compactor_workspace_id
      }

      env {
        name  = "ARCO_FLOW_DEBUG"
        value = var.environment == "dev" ? "1" : "0"
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  lifecycle {
    ignore_changes = [
      client,
      client_version,
    ]
  }
}

# ============================================================================
# Cloud Scheduler (Compactor Trigger - optional)
# ============================================================================

# Optional: Trigger compactor on a schedule (for environments without always-on)
resource "google_cloud_scheduler_job" "compactor_trigger" {
  count = var.compactor_min_instances == 0 ? 1 : 0

  name        = "arco-compactor-trigger-${var.environment}"
  project     = var.project_id
  region      = var.region
  description = "Triggers Arco compactor periodically"
  schedule    = "*/5 * * * *" # Every 5 minutes

  http_target {
    uri         = "${google_cloud_run_v2_service.compactor.uri}/compact"
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.invoker.email
      audience              = google_cloud_run_v2_service.compactor.uri
    }
  }

  retry_config {
    retry_count = 3
  }
}

# ============================================================================
# Outputs
# ============================================================================

output "api_url" {
  description = "URL of the API service"
  value       = google_cloud_run_v2_service.api.uri
}

output "api_service_name" {
  description = "Name of the API Cloud Run service"
  value       = google_cloud_run_v2_service.api.name
}

output "compactor_url" {
  description = "URL of the Compactor service (internal only)"
  value       = google_cloud_run_v2_service.compactor.uri
}

output "compactor_service_name" {
  description = "Name of the Compactor Cloud Run service"
  value       = google_cloud_run_v2_service.compactor.name
}

output "flow_compactor_url" {
  description = "URL of the Arco Flow orchestration compactor service (internal only)"
  value       = google_cloud_run_v2_service.flow_compactor.uri
}

output "flow_compactor_service_name" {
  description = "Name of the Arco Flow orchestration compactor Cloud Run service"
  value       = google_cloud_run_v2_service.flow_compactor.name
}
