# Arco IAM Configuration
#
# Implements least-privilege service accounts for Arco services.
#
# ## Security Design
#
# Each service gets its own service account with minimal permissions:
# - API Service: Read/write storage for tenant data, read secrets
# - Compactor Service: Read/write storage (sole Parquet writer), read secrets
# - Invoker: Used by Cloud Scheduler/Pub/Sub to trigger services
#
# ## Principle of Least Privilege
#
# - No service account has `roles/owner` or `roles/editor`
# - Storage permissions are scoped to specific buckets
# - Secret access is limited to specific secrets
# - Compactor cannot invoke API, API cannot compact

# ============================================================================
# API Service Account
# ============================================================================

resource "google_service_account" "api" {
  account_id   = "arco-api-${var.environment}"
  display_name = "Arco API Service (${var.environment})"
  description  = "Service account for Arco API - handles DDL, reads, signed URLs"
  project      = var.project_id
}

# Gate 5: Prefix-scoped IAM - see iam_conditions.tf for detailed bindings
#
# API can write to: ledger/, locks/, commits/, manifests/
# Compactor can write to: state/, l0/, manifests/
# Both can read all prefixes
#
# NOTE: Prefix scoping MUST be anchored (no `contains()`); see iam_conditions.tf.
#
# REMOVED (Gate 5 violation): Bucket-wide objectUser grants
# The following resource was removed and replaced with prefix-scoped bindings:
# resource "google_storage_bucket_iam_member" "api_catalog_access" { ... }

# API can read JWT secret (if using HS256)
resource "google_secret_manager_secret_iam_member" "api_jwt_secret" {
  count     = var.jwt_secret_name != "" ? 1 : 0
  project   = var.project_id
  secret_id = var.jwt_secret_name
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.api.email}"
}

resource "google_secret_manager_secret_iam_member" "compactor_tenant_secret" {
  count     = var.tenant_secret_name != "" && var.environment != "dev" ? 1 : 0
  project   = var.project_id
  secret_id = var.tenant_secret_name
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.compactor.email}"
}

# ============================================================================
# Compactor Service Accounts (Split for Gate 5 Defense-in-Depth)
# ============================================================================
#
# Gate 5 Patch 9: Split compactor into two service accounts to prevent
# accidental listing in the fast path:
#
# | SA                     | Permissions                          | Purpose               |
# |------------------------|--------------------------------------|-----------------------|
# | compactor-fastpath     | state/, l0/, manifests/ write; NO list | Notification consumer |
# | compactor-antientropy  | ledger/ list; state/ read            | Anti-entropy job      |
#
# This makes "oops, compactor started listing in hot path" a deploy-time
# IAM failure, not just a code review issue.

# Fast-path compactor: handles notifications, writes state, NO listing
resource "google_service_account" "compactor" {
  account_id   = "arco-compactor-${var.environment}"
  display_name = "Arco Compactor Fast-Path (${var.environment})"
  description  = "Fast-path compactor - sole writer of Parquet state, NO list permission"
  project      = var.project_id
}

# Anti-entropy compactor: can list ledger to find missed events
resource "google_service_account" "compactor_antientropy" {
  account_id   = "arco-compactor-ae-${var.environment}"
  display_name = "Arco Compactor Anti-Entropy (${var.environment})"
  description  = "Anti-entropy job - can list ledger/ to discover missed events"
  project      = var.project_id
}

# ============================================================================
# Flow Control-Plane Service Accounts
# ============================================================================

resource "google_service_account" "flow_dispatcher" {
  count        = local.flow_services_enabled ? 1 : 0
  account_id   = "arco-flow-disp-${var.environment}"
  display_name = "Arco Flow Dispatcher (${var.environment})"
  description  = "Flow dispatcher service account"
  project      = var.project_id
}

resource "google_service_account" "flow_sweeper" {
  count        = local.flow_services_enabled ? 1 : 0
  account_id   = "arco-flow-sweep-${var.environment}"
  display_name = "Arco Flow Sweeper (${var.environment})"
  description  = "Flow sweeper service account"
  project      = var.project_id
}

resource "google_service_account" "flow_timer_ingest" {
  count        = local.flow_services_enabled ? 1 : 0
  account_id   = "arco-flow-timer-${var.environment}"
  display_name = "Arco Flow Timer Ingest (${var.environment})"
  description  = "Flow timer callback ingest service account"
  project      = var.project_id
}

resource "google_service_account" "flow_tasks_oidc" {
  count        = local.flow_services_enabled ? 1 : 0
  account_id   = "arco-flow-tasks-${var.environment}"
  display_name = "Arco Flow Cloud Tasks OIDC (${var.environment})"
  description  = "OIDC identity used by Cloud Tasks callbacks"
  project      = var.project_id
}

# REMOVED (Gate 5 violation): Bucket-wide objectUser grants
# The following resource was removed and replaced with prefix-scoped bindings:
# resource "google_storage_bucket_iam_member" "compactor_catalog_access" { ... }
# See iam_conditions.tf for the new prefix-scoped bindings.

# ============================================================================
# Cloud Run Invoker (for scheduled jobs)
# ============================================================================

resource "google_service_account" "invoker" {
  account_id   = "arco-invoker-${var.environment}"
  display_name = "Arco Service Invoker (${var.environment})"
  description  = "Service account for invoking Arco services (Cloud Scheduler, Pub/Sub)"
  project      = var.project_id
}

# ============================================================================
# Arco Flow Service Accounts
# ============================================================================

resource "google_service_account" "flow_controller" {
  account_id   = "arco-flow-controller-${var.environment}"
  display_name = "Arco Flow Controller (${var.environment})"
  description  = "Service account for Arco Flow controllers (dispatcher/sweeper/automation)"
  project      = var.project_id
}

resource "google_service_account" "flow_task_invoker" {
  account_id   = "arco-flow-task-invoker-${var.environment}"
  display_name = "Arco Flow Task Invoker (${var.environment})"
  description  = "Service account used for Cloud Tasks OIDC invocations into Cloud Run"
  project      = var.project_id
}

resource "google_service_account" "flow_worker" {
  account_id   = "arco-flow-worker-${var.environment}"
  display_name = "Arco Flow Worker (${var.environment})"
  description  = "Service account for Arco Flow worker service (executes dispatched tasks)"
  project      = var.project_id
}

# ============================================================================
# Custom IAM Roles
# ============================================================================

# Read-only object access without list permission (enforces no-list invariant).
resource "google_project_iam_custom_role" "storage_object_reader_no_list" {
  role_id     = "storageObjectReaderNoList"
  title       = "Storage Object Reader (No List)"
  description = "Read individual objects without list capability"
  permissions = ["storage.objects.get"]
  project     = var.project_id
}

# Invoker can trigger Cloud Run services
resource "google_cloud_run_v2_service_iam_member" "invoker_compactor" {
  count    = var.environment != "" ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.compactor.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.invoker.email}"
}

resource "google_cloud_run_v2_service_iam_member" "compactor_antientropy_invoker" {
  count    = var.environment != "" ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.compactor.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.compactor_antientropy.email}"
}

resource "google_cloud_run_v2_service_iam_member" "invoker_flow_dispatcher" {
  count    = local.flow_services_enabled ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.flow_dispatcher[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.invoker.email}"
}

resource "google_cloud_run_v2_service_iam_member" "invoker_flow_sweeper" {
  count    = local.flow_services_enabled ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.flow_sweeper[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.invoker.email}"
}

resource "google_cloud_run_v2_service_iam_member" "flow_tasks_timer_ingest_invoker" {
  count    = local.flow_services_enabled ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.flow_timer_ingest[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.flow_tasks_oidc[0].email}"
}

resource "google_project_iam_member" "flow_dispatcher_cloudtasks_enqueuer" {
  count   = local.flow_services_enabled ? 1 : 0
  project = var.project_id
  role    = "roles/cloudtasks.enqueuer"
  member  = "serviceAccount:${google_service_account.flow_dispatcher[0].email}"
}

resource "google_project_iam_member" "flow_sweeper_cloudtasks_enqueuer" {
  count   = local.flow_services_enabled ? 1 : 0
  project = var.project_id
  role    = "roles/cloudtasks.enqueuer"
  member  = "serviceAccount:${google_service_account.flow_sweeper[0].email}"
}

resource "google_service_account_iam_member" "flow_dispatcher_act_as_tasks_oidc" {
  count              = local.flow_services_enabled ? 1 : 0
  service_account_id = google_service_account.flow_tasks_oidc[0].name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.flow_dispatcher[0].email}"
}

resource "google_service_account_iam_member" "flow_sweeper_act_as_tasks_oidc" {
  count              = local.flow_services_enabled ? 1 : 0
  service_account_id = google_service_account.flow_tasks_oidc[0].name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.flow_sweeper[0].email}"
}

resource "google_storage_bucket_iam_member" "flow_dispatcher_storage_access" {
  count  = local.flow_services_enabled ? 1 : 0
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.flow_dispatcher[0].email}"
}

resource "google_storage_bucket_iam_member" "flow_sweeper_storage_access" {
  count  = local.flow_services_enabled ? 1 : 0
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.flow_sweeper[0].email}"
}

resource "google_storage_bucket_iam_member" "flow_timer_ingest_storage_access" {
  count  = local.flow_services_enabled ? 1 : 0
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.flow_timer_ingest[0].email}"
}

# ============================================================================
# Public Access (optional - for API when public)
# ============================================================================

# Allow unauthenticated access to API (if public)
resource "google_cloud_run_v2_service_iam_member" "api_public" {
  count    = var.api_public ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.api.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# ============================================================================
# Outputs
# ============================================================================

output "api_service_account_email" {
  description = "Email of the API service account"
  value       = google_service_account.api.email
}

output "compactor_service_account_email" {
  description = "Email of the Compactor fast-path service account"
  value       = google_service_account.compactor.email
}

output "compactor_antientropy_service_account_email" {
  description = "Email of the Compactor anti-entropy service account"
  value       = google_service_account.compactor_antientropy.email
}

output "invoker_service_account_email" {
  description = "Email of the Invoker service account"
  value       = google_service_account.invoker.email
}

output "flow_dispatcher_service_account_email" {
  description = "Email of the Flow dispatcher service account"
  value       = local.flow_services_enabled ? google_service_account.flow_dispatcher[0].email : ""
}

output "flow_sweeper_service_account_email" {
  description = "Email of the Flow sweeper service account"
  value       = local.flow_services_enabled ? google_service_account.flow_sweeper[0].email : ""
}

output "flow_timer_ingest_service_account_email" {
  description = "Email of the Flow timer-ingest service account"
  value       = local.flow_services_enabled ? google_service_account.flow_timer_ingest[0].email : ""
}

output "flow_tasks_oidc_service_account_email" {
  description = "Email of the Flow Cloud Tasks OIDC service account"
  value       = local.flow_services_enabled ? google_service_account.flow_tasks_oidc[0].email : ""
}
