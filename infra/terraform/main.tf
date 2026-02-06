# Arco Infrastructure - GCP
#
# Terraform configuration for deploying Arco to Google Cloud Platform.
#
# ## Architecture
#
# - Storage: GCS bucket for catalog metadata (ledger + snapshots)
# - Compute: Cloud Run services for API and Compactor
# - IAM: Least-privilege service accounts per service
# - Security: Secret Manager for JWT secrets
#
# ## Deployment Order
#
# CRITICAL: Compactor must be healthy before API accepts traffic.
# Use the deploy script (scripts/deploy.sh) to ensure proper ordering.
#
# ## Usage
#
# ```bash
# terraform init
# terraform plan -var-file=environments/prod.tfvars
# terraform apply -var-file=environments/prod.tfvars
# ```

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.15"
    }
  }

  # Uncomment and configure for remote state
  # backend "gcs" {
  #   bucket = "your-terraform-state-bucket"
  #   prefix = "arco"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ============================================================================
# Storage
# ============================================================================

# Storage bucket for catalog metadata
resource "google_storage_bucket" "catalog" {
  name     = "${var.project_id}-arco-catalog-${var.environment}"
  location = var.region
  project  = var.project_id

  # Uniform access control (recommended for security)
  uniform_bucket_level_access = true

  # SECURITY: Prevent any public access to bucket data.
  # This is a critical defense-in-depth control that ensures bucket data
  # cannot be accidentally exposed even if ACLs are misconfigured.
  public_access_prevention = "enforced"

  # Enable versioning for data protection
  versioning {
    enabled = true
  }

  # Lifecycle rules for cost optimization
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  # Delete old versions after 90 days
  lifecycle_rule {
    condition {
      age                = 90
      with_state         = "ARCHIVED"
      num_newer_versions = 3
    }
    action {
      type = "Delete"
    }
  }

  # Prevent accidental deletion in production
  force_destroy = var.environment != "prod"

  labels = {
    environment = var.environment
    service     = "arco"
    component   = "catalog"
  }
}

# ============================================================================
# Secret Manager (optional - for JWT secrets)
# ============================================================================

# Create secret placeholder (actual value set manually or via CI/CD)
resource "google_secret_manager_secret" "jwt_secret" {
  count     = var.jwt_secret_name != "" ? 1 : 0
  project   = var.project_id
  secret_id = var.jwt_secret_name

  replication {
    auto {}
  }

  labels = {
    environment = var.environment
    service     = "arco"
  }
}

# ============================================================================
# Outputs
# ============================================================================

output "catalog_bucket" {
  description = "Name of the catalog storage bucket"
  value       = google_storage_bucket.catalog.name
}

output "environment" {
  description = "Deployed environment"
  value       = var.environment
}

output "region" {
  description = "Deployed region"
  value       = var.region
}
