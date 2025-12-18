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

# API needs read/write access to catalog bucket (for Tier-1 operations)
resource "google_storage_bucket_iam_member" "api_catalog_access" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.api.email}"
}

# API can read JWT secret (if using HS256)
resource "google_secret_manager_secret_iam_member" "api_jwt_secret" {
  count     = var.jwt_secret_name != "" ? 1 : 0
  project   = var.project_id
  secret_id = var.jwt_secret_name
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.api.email}"
}

# ============================================================================
# Compactor Service Account
# ============================================================================

resource "google_service_account" "compactor" {
  account_id   = "arco-compactor-${var.environment}"
  display_name = "Arco Compactor Service (${var.environment})"
  description  = "Service account for Arco Compactor - sole writer of Parquet state"
  project      = var.project_id
}

# Compactor needs read/write access to catalog bucket (reads ledger, writes state)
resource "google_storage_bucket_iam_member" "compactor_catalog_access" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.compactor.email}"
}

# ============================================================================
# Cloud Run Invoker (for scheduled jobs)
# ============================================================================

resource "google_service_account" "invoker" {
  account_id   = "arco-invoker-${var.environment}"
  display_name = "Arco Service Invoker (${var.environment})"
  description  = "Service account for invoking Arco services (Cloud Scheduler, Pub/Sub)"
  project      = var.project_id
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
  description = "Email of the Compactor service account"
  value       = google_service_account.compactor.email
}

output "invoker_service_account_email" {
  description = "Email of the Invoker service account"
  value       = google_service_account.invoker.email
}
