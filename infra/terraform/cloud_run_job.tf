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
