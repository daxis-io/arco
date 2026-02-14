# Arco Flow Cloud Tasks Queues + OIDC Callback IAM
#
# This file defines queue infrastructure for:
# - Worker dispatch requests (`ARCO_FLOW_QUEUE`)
# - Timer callback delivery (`ARCO_FLOW_TIMER_QUEUE`)
#
# Dispatcher runtime contract:
# - Dispatcher enqueues tasks with OIDC service account identity
# - Dispatcher caller SA (`arco-api-*`) gets `iam.serviceAccounts.actAs` on invoker SA (see iam.tf)
# - Timer callback endpoint validates OIDC issuer/audience and rejects unauthorized traffic

data "google_project" "current" {
  project_id = var.project_id
}

locals {
  cloud_tasks_service_agent = "service-${data.google_project.current.number}@gcp-sa-cloudtasks.iam.gserviceaccount.com"
}

resource "google_cloud_tasks_queue" "flow_dispatch" {
  name     = var.flow_dispatch_queue_name
  location = var.region
  project  = var.project_id

  rate_limits {
    max_dispatches_per_second = var.flow_queue_max_dispatches_per_second
    max_concurrent_dispatches = var.flow_queue_max_concurrent_dispatches
  }

  retry_config {
    max_attempts       = var.flow_queue_max_attempts
    max_retry_duration = "${var.flow_queue_max_retry_duration_seconds}s"
    min_backoff        = "${var.flow_queue_min_backoff_seconds}s"
    max_backoff        = "${var.flow_queue_max_backoff_seconds}s"
  }

  stackdriver_logging_config {
    sampling_ratio = 1.0
  }
}

resource "google_cloud_tasks_queue" "flow_timer" {
  name     = var.flow_timer_queue_name
  location = var.region
  project  = var.project_id

  rate_limits {
    max_dispatches_per_second = var.flow_queue_max_dispatches_per_second
    max_concurrent_dispatches = var.flow_queue_max_concurrent_dispatches
  }

  retry_config {
    max_attempts       = var.flow_queue_max_attempts
    max_retry_duration = "${var.flow_queue_max_retry_duration_seconds}s"
    min_backoff        = "${var.flow_queue_min_backoff_seconds}s"
    max_backoff        = "${var.flow_queue_max_backoff_seconds}s"
  }

  stackdriver_logging_config {
    sampling_ratio = 1.0
  }
}

# Cloud Tasks must be allowed to mint OIDC tokens from the configured invoker SA.
resource "google_service_account_iam_member" "cloud_tasks_can_impersonate_invoker" {
  service_account_id = google_service_account.invoker.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${local.cloud_tasks_service_agent}"
}

# Optional: if dispatcher service is managed by this stack, grant invoker SA explicit invoke permission.
resource "google_cloud_run_v2_service_iam_member" "invoker_dispatcher" {
  count    = var.flow_dispatcher_service_name != "" ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = var.flow_dispatcher_service_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.invoker.email}"
}

output "flow_dispatch_queue_name" {
  description = "Cloud Tasks queue name used by dispatcher for worker execution payloads"
  value       = google_cloud_tasks_queue.flow_dispatch.name
}

output "flow_timer_queue_name" {
  description = "Cloud Tasks queue name used for timer callback payloads"
  value       = google_cloud_tasks_queue.flow_timer.name
}
