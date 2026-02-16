# Arco Cloud Tasks Queues
#
# Queues for dispatching work to workers and for timer callbacks into controllers.

resource "google_cloud_tasks_queue" "flow_dispatch" {
  name     = "arco-flow-dispatch"
  location = var.region
  project  = var.project_id
}

resource "google_cloud_tasks_queue" "flow_timer" {
  name     = "arco-flow-timer"
  location = var.region
  project  = var.project_id
}

