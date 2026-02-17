# Arco Flow control-plane Cloud Run services.

locals {
  flow_services_enabled        = var.flow_dispatcher_image != "" && var.flow_sweeper_image != "" && var.flow_timer_ingest_image != "" && var.flow_dispatch_target_url != "" && var.flow_tenant_id != "" && var.flow_workspace_id != ""
  flow_dispatcher_run_audience = "arco-flow-dispatcher-run-${var.environment}"
  flow_sweeper_run_audience    = "arco-flow-sweeper-run-${var.environment}"
  flow_timer_ingest_audience   = "arco-flow-timer-ingest-${var.environment}"
}

resource "google_cloud_run_v2_service" "flow_timer_ingest" {
  count    = local.flow_services_enabled ? 1 : 0
  name     = "arco-flow-timer-ingest-${var.environment}"
  location = var.region
  project  = var.project_id
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.flow_timer_ingest[0].email

    scaling {
      min_instance_count = var.flow_min_instances
      max_instance_count = var.flow_max_instances
    }

    containers {
      image = var.flow_timer_ingest_image

      resources {
        limits = {
          cpu    = var.flow_cpu
          memory = var.flow_memory
        }
      }

      env {
        name  = "ARCO_TENANT_ID"
        value = var.flow_tenant_id
      }
      env {
        name  = "ARCO_WORKSPACE_ID"
        value = var.flow_workspace_id
      }
      env {
        name  = "ARCO_STORAGE_BUCKET"
        value = google_storage_bucket.catalog.name
      }
      env {
        name  = "ARCO_INTERNAL_AUTH_ENFORCE"
        value = "true"
      }
      env {
        name  = "ARCO_INTERNAL_AUTH_ISSUER"
        value = "https://accounts.google.com"
      }
      env {
        name  = "ARCO_INTERNAL_AUTH_AUDIENCE"
        value = local.flow_timer_ingest_audience
      }
      env {
        name  = "ARCO_INTERNAL_AUTH_ALLOWED_EMAILS"
        value = google_service_account.flow_tasks_oidc[0].email
      }
    }
  }
}

resource "google_cloud_scheduler_job" "flow_dispatcher_run" {
  count       = local.flow_services_enabled ? 1 : 0
  name        = "arco-flow-dispatcher-run-${var.environment}"
  project     = var.project_id
  region      = var.region
  description = "Triggers flow dispatcher reconciliation"
  schedule    = "*/1 * * * *"

  http_target {
    uri         = "${google_cloud_run_v2_service.flow_dispatcher.uri}/run"
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.invoker.email
      audience              = local.flow_dispatcher_run_audience
    }
  }
}

resource "google_cloud_scheduler_job" "flow_sweeper_run" {
  count       = local.flow_services_enabled ? 1 : 0
  name        = "arco-flow-sweeper-run-${var.environment}"
  project     = var.project_id
  region      = var.region
  description = "Triggers flow sweeper anti-entropy reconciliation"
  schedule    = "*/5 * * * *"

  http_target {
    uri         = "${google_cloud_run_v2_service.flow_sweeper.uri}/run"
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.invoker.email
      audience              = local.flow_sweeper_run_audience
    }
  }
}

output "flow_dispatcher_url" {
  description = "Flow dispatcher service URL"
  value       = local.flow_services_enabled ? google_cloud_run_v2_service.flow_dispatcher.uri : ""
}

output "flow_sweeper_url" {
  description = "Flow sweeper service URL"
  value       = local.flow_services_enabled ? google_cloud_run_v2_service.flow_sweeper.uri : ""
}

output "flow_timer_ingest_url" {
  description = "Flow timer-ingest service URL"
  value       = local.flow_services_enabled ? google_cloud_run_v2_service.flow_timer_ingest[0].uri : ""
}
