# Arco Cloud Run Jobs
#
# Deploys batch-style jobs that require different IAM than the compactor service.

# ============================================================================
# Compactor Anti-Entropy Job (periodic/batch)
# ============================================================================

resource "google_cloud_run_v2_job" "compactor_antientropy" {
  name     = "arco-compactor-antientropy-${var.environment}"
  location = var.region
  project  = var.project_id

  template {
    template {
      service_account = google_service_account.compactor_antientropy.email

      containers {
        image = var.compactor_image

        args = [
          "anti-entropy",
          "--tenant-id",
          var.compactor_tenant_id,
          "--workspace-id",
          var.compactor_workspace_id,
          "--storage-bucket",
          google_storage_bucket.catalog.name,
          "--domain",
          var.anti_entropy_domain,
          "--max-objects-per-run",
          tostring(var.anti_entropy_max_objects_per_run),
        ]

        env {
          name  = "ARCO_STORAGE_BUCKET"
          value = google_storage_bucket.catalog.name
        }

        env {
          name  = "ARCO_COMPACTOR_URL"
          value = google_cloud_run_v2_service.compactor.uri
        }

        env {
          name  = "ARCO_COMPACTOR_AUDIENCE"
          value = google_cloud_run_v2_service.compactor.uri
        }

        env {
          name  = "ARCO_ENVIRONMENT"
          value = var.environment
        }
      }
    }
  }
}

resource "google_cloud_run_v2_job_iam_member" "compactor_antientropy_invoker" {
  name     = google_cloud_run_v2_job.compactor_antientropy.name
  location = var.region
  project  = var.project_id
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.invoker.email}"
}

# ============================================================================
# Cloud Scheduler (Anti-Entropy Trigger)
# ============================================================================

resource "google_cloud_scheduler_job" "compactor_antientropy_trigger" {
  count       = var.background_automation_enabled ? 1 : 0
  name        = "arco-compactor-antientropy-trigger-${var.environment}"
  project     = var.project_id
  region      = var.region
  description = "Triggers Arco compactor anti-entropy job periodically"
  schedule    = var.anti_entropy_schedule

  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.compactor_antientropy.name}:run"
    http_method = "POST"

    oauth_token {
      service_account_email = google_service_account.invoker.email
    }
  }

  retry_config {
    retry_count = 3
  }
}

# ============================================================================
# Control-Store Worker Job (projection drain, layout maintenance, GC)
# ============================================================================
#
# Runs `arco_control_store_worker` once per trigger. Each run drains the catalog
# projection outbox, publishes pending L0 layout maintenance for every control
# domain, and collects a bounded number of GC pages per domain.
#
# The job runs under the API service account: that account is the SOLE writer
# of the `control/` prefix (see iam_conditions.tf). Never bind this job to the
# compactor service accounts.
#
# A cron cadence cannot meet ADR-043's projection-lag objective (p99 <= 10 s):
# with a 5-minute schedule the worst-case drain latency is the schedule period.
# Queue-driven wake (a Pub/Sub or Cloud Tasks trigger fired by commit) remains
# cutover work; until then the API's post-commit fail-open drain provides the
# fast path and this job is the restart-safe anti-entropy backstop.

locals {
  control_store_worker_enabled = (
    var.control_store_worker_image != ""
    && var.control_store_tenant_id != ""
    && var.control_store_workspace_id != ""
    && var.control_store_maintenance_binding_secret != ""
  )
}

resource "google_cloud_run_v2_job" "control_store_worker" {
  count    = local.control_store_worker_enabled ? 1 : 0
  name     = "arco-control-store-worker-${var.environment}"
  location = var.region
  project  = var.project_id

  template {
    template {
      service_account = google_service_account.api.email

      containers {
        image = var.control_store_worker_image

        env {
          name  = "ARCO_STORAGE_BUCKET"
          value = google_storage_bucket.catalog.name
        }

        env {
          name  = "ARCO_CATALOG_CONTROL_V1_TENANT_ID"
          value = var.control_store_tenant_id
        }

        env {
          name  = "ARCO_CATALOG_CONTROL_V1_WORKSPACE_ID"
          value = var.control_store_workspace_id
        }

        env {
          name  = "ARCO_ENVIRONMENT"
          value = var.environment
        }

        env {
          name  = "ARCO_LOG_FORMAT"
          value = "json"
        }

        env {
          name = "ARCO_CONTROL_STORE_MAINTENANCE_BINDING"
          value_source {
            secret_key_ref {
              secret  = var.control_store_maintenance_binding_secret
              version = "latest"
            }
          }
        }
      }
    }
  }

  depends_on = [google_secret_manager_secret_iam_member.api_control_store_maintenance_binding_secret]
}

# The API service account must be able to read the binding secret that the job
# mounts. Mirrors google_secret_manager_secret_iam_member.api_jwt_secret in
# iam.tf; it lives here because it exists only with the job.
resource "google_secret_manager_secret_iam_member" "api_control_store_maintenance_binding_secret" {
  count     = local.control_store_worker_enabled ? 1 : 0
  project   = var.project_id
  secret_id = var.control_store_maintenance_binding_secret
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.api.email}"
}

resource "google_cloud_run_v2_job_iam_member" "control_store_worker_invoker" {
  count    = local.control_store_worker_enabled ? 1 : 0
  name     = google_cloud_run_v2_job.control_store_worker[0].name
  location = var.region
  project  = var.project_id
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.invoker.email}"
}

# ============================================================================
# Cloud Scheduler (Control-Store Worker Trigger)
# ============================================================================

resource "google_cloud_scheduler_job" "control_store_worker_trigger" {
  count       = local.control_store_worker_enabled && var.background_automation_enabled ? 1 : 0
  name        = "arco-control-store-worker-trigger-${var.environment}"
  project     = var.project_id
  region      = var.region
  description = "Triggers the Arco control-store worker job (projection drain, maintenance, GC)"
  schedule    = var.control_store_worker_schedule

  http_target {
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.control_store_worker[0].name}:run"
    http_method = "POST"

    oauth_token {
      service_account_email = google_service_account.invoker.email
    }
  }

  retry_config {
    retry_count = 3
  }
}
