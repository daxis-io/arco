# Arco Flow Cloud Tasks queues.
#
# These queues are declarative source-of-truth for dispatch and timer callbacks.

locals {
  flow_dispatch_queue_name = "arco-flow-dispatch-${var.environment}"
  flow_timer_queue_name    = "arco-flow-timer-${var.environment}"
}

resource "google_cloud_tasks_queue" "flow_dispatch" {
  name     = local.flow_dispatch_queue_name
  project  = var.project_id
  location = var.region

  rate_limits {
    max_dispatches_per_second = 200
    max_concurrent_dispatches = 200
  }

  retry_config {
    max_attempts       = 8
    max_retry_duration = "900s"
    min_backoff        = "5s"
    max_backoff        = "300s"
    max_doublings      = 5
  }
}

resource "google_cloud_tasks_queue" "flow_timer" {
  name     = local.flow_timer_queue_name
  project  = var.project_id
  location = var.region

  rate_limits {
    max_dispatches_per_second = 100
    max_concurrent_dispatches = 100
  }

  retry_config {
    max_attempts       = 10
    max_retry_duration = "3600s"
    min_backoff        = "10s"
    max_backoff        = "600s"
    max_doublings      = 6
  }
}

output "flow_dispatch_queue_name" {
  description = "Flow dispatch Cloud Tasks queue name"
  value       = google_cloud_tasks_queue.flow_dispatch.name
}

output "flow_timer_queue_name" {
  description = "Flow timer Cloud Tasks queue name"
  value       = google_cloud_tasks_queue.flow_timer.name
}
