# Arco Flow control-plane Cloud Run services.

locals {
  flow_services_enabled                  = var.flow_dispatcher_image != "" && var.flow_sweeper_image != "" && var.flow_timer_ingest_image != "" && var.flow_worker_image != "" && var.flow_tenant_id != "" && var.flow_workspace_id != ""
  flow_automation_reconciler_enabled     = var.flow_automation_reconciler_image != "" && var.flow_tenant_id != "" && var.flow_workspace_id != ""
  api_service_url                        = "https://arco-api-${var.environment}-${local.project_number}.${var.region}.run.app"
  flow_dispatcher_service_url            = "https://arco-flow-dispatcher-${var.environment}-${local.project_number}.${var.region}.run.app"
  flow_sweeper_service_url               = "https://arco-flow-sweeper-${var.environment}-${local.project_number}.${var.region}.run.app"
  flow_timer_ingest_service_url          = "https://arco-flow-timer-ingest-${var.environment}-${local.project_number}.${var.region}.run.app"
  flow_automation_reconciler_service_url = "https://arco-flow-automation-reconciler-${var.environment}-${local.project_number}.${var.region}.run.app"
  flow_worker_service_url                = "https://arco-flow-worker-${var.environment}-${local.project_number}.${var.region}.run.app"
  flow_worker_dispatch_url               = "${local.flow_worker_service_url}/dispatch"
  flow_timer_ingest_internal_url         = "${local.flow_timer_ingest_service_url}/internal/timers/fired"
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
        name  = "ARCO_FLOW_COMPACTOR_URL"
        value = google_cloud_run_v2_service.flow_compactor.uri
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
        value = local.flow_timer_ingest_service_url
      }
      env {
        name  = "ARCO_INTERNAL_AUTH_ALLOWED_EMAILS"
        value = google_service_account.flow_task_invoker.email
      }
    }
  }
}

resource "google_cloud_run_v2_service" "flow_automation_reconciler" {
  count    = local.flow_automation_reconciler_enabled ? 1 : 0
  name     = "arco-flow-automation-reconciler-${var.environment}"
  location = var.region
  project  = var.project_id
  ingress  = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  template {
    service_account = google_service_account.flow_controller.email

    scaling {
      min_instance_count = var.background_automation_enabled ? 1 : 0
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
      image = var.flow_automation_reconciler_image

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        cpu_idle          = !var.background_automation_enabled
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
        name  = "ARCO_FLOW_COMPACTOR_URL"
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

resource "google_cloud_scheduler_job" "flow_automation_reconciler_run" {
  count       = local.flow_automation_reconciler_enabled && var.background_automation_enabled ? 1 : 0
  name        = "arco-flow-automation-reconciler-run-${var.environment}"
  project     = var.project_id
  region      = var.region
  description = "Triggers flow automation reconciliation"
  schedule    = var.flow_automation_reconciler_schedule

  http_target {
    uri         = "${local.flow_automation_reconciler_service_url}/run"
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.invoker.email
      audience              = local.flow_automation_reconciler_service_url
    }
  }

  depends_on = [
    google_cloud_run_v2_service.flow_automation_reconciler,
    google_cloud_run_v2_service_iam_member.invoker_flow_automation_reconciler,
  ]
}

resource "google_cloud_scheduler_job" "flow_dispatcher_run" {
  count       = local.flow_services_enabled && var.background_automation_enabled ? 1 : 0
  name        = "arco-flow-dispatcher-run-${var.environment}"
  project     = var.project_id
  region      = var.region
  description = "Triggers flow dispatcher reconciliation"
  schedule    = "*/1 * * * *"

  http_target {
    uri         = "${local.flow_dispatcher_service_url}/run"
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.invoker.email
      audience              = local.flow_dispatcher_service_url
    }
  }

  depends_on = [
    google_cloud_run_v2_service.flow_dispatcher,
    google_cloud_run_v2_service_iam_member.invoker_flow_dispatcher,
  ]
}

resource "google_cloud_scheduler_job" "flow_sweeper_run" {
  count       = local.flow_services_enabled && var.background_automation_enabled ? 1 : 0
  name        = "arco-flow-sweeper-run-${var.environment}"
  project     = var.project_id
  region      = var.region
  description = "Triggers flow sweeper anti-entropy reconciliation"
  schedule    = "*/5 * * * *"

  http_target {
    uri         = "${local.flow_sweeper_service_url}/run"
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.invoker.email
      audience              = local.flow_sweeper_service_url
    }
  }

  depends_on = [
    google_cloud_run_v2_service.flow_sweeper,
    google_cloud_run_v2_service_iam_member.invoker_flow_sweeper,
  ]
}

output "flow_dispatcher_url" {
  description = "Flow dispatcher service URL"
  value       = local.flow_services_enabled ? google_cloud_run_v2_service.flow_dispatcher[0].uri : ""
}

output "flow_automation_reconciler_url" {
  description = "Flow automation reconciler service URL"
  value       = local.flow_automation_reconciler_enabled ? google_cloud_run_v2_service.flow_automation_reconciler[0].uri : ""
}

output "flow_sweeper_url" {
  description = "Flow sweeper service URL"
  value       = local.flow_services_enabled ? google_cloud_run_v2_service.flow_sweeper[0].uri : ""
}

output "flow_timer_ingest_url" {
  description = "Flow timer-ingest service URL"
  value       = local.flow_services_enabled ? google_cloud_run_v2_service.flow_timer_ingest[0].uri : ""
}
