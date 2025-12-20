# Arco Terraform Variables
#
# Central variable definitions for the Arco infrastructure.

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

# ============================================================================
# Container Images
# ============================================================================

variable "api_image" {
  description = "Container image for Arco API service"
  type        = string
}

variable "compactor_image" {
  description = "Container image for Arco Compactor service"
  type        = string
}

# ============================================================================
# Compactor Scoped Configuration
# ============================================================================

variable "compactor_tenant_id" {
  description = "Tenant ID for compactor and anti-entropy jobs"
  type        = string
}

variable "compactor_workspace_id" {
  description = "Workspace ID for compactor and anti-entropy jobs"
  type        = string
}

# ============================================================================
# Anti-Entropy Configuration
# ============================================================================

variable "anti_entropy_domain" {
  description = "Domain to scan during anti-entropy runs"
  type        = string
  default     = "catalog"
}

variable "anti_entropy_max_objects_per_run" {
  description = "Maximum objects to scan per anti-entropy run"
  type        = number
  default     = 1000
}

variable "anti_entropy_schedule" {
  description = "Cron schedule for anti-entropy job"
  type        = string
  default     = "*/15 * * * *"
}

# ============================================================================
# Cloud Run Configuration
# ============================================================================

variable "api_cpu" {
  description = "CPU allocation for API service (e.g., '1', '2')"
  type        = string
  default     = "1"
}

variable "api_memory" {
  description = "Memory allocation for API service (e.g., '512Mi', '1Gi')"
  type        = string
  default     = "512Mi"
}

variable "api_min_instances" {
  description = "Minimum instances for API service"
  type        = number
  default     = 0
}

variable "api_max_instances" {
  description = "Maximum instances for API service"
  type        = number
  default     = 10
}

variable "compactor_cpu" {
  description = "CPU allocation for Compactor service"
  type        = string
  default     = "2"
}

variable "compactor_memory" {
  description = "Memory allocation for Compactor service"
  type        = string
  default     = "1Gi"
}

variable "compactor_min_instances" {
  description = "Minimum instances for Compactor service (1 for always-on)"
  type        = number
  default     = 1
}

variable "compactor_max_instances" {
  description = "Maximum instances for Compactor service"
  type        = number
  default     = 1
}

# ============================================================================
# Security Configuration
# ============================================================================

variable "allowed_cors_origins" {
  description = "CORS allowed origins (comma-separated or '*' for dev)"
  type        = string
  default     = ""
}

variable "jwt_audience" {
  description = "Expected JWT audience claim"
  type        = string
  default     = ""
}

variable "jwt_issuer" {
  description = "Expected JWT issuer claim"
  type        = string
  default     = ""
}

variable "jwt_secret_name" {
  description = "Secret Manager secret name containing JWT secret"
  type        = string
  default     = "arco-jwt-secret"
}

# ============================================================================
# Networking
# ============================================================================

variable "api_public" {
  description = "Whether API is publicly accessible (false = internal only)"
  type        = bool
  default     = false
}

variable "vpc_connector_name" {
  description = "Serverless VPC connector name (optional, for private services)"
  type        = string
  default     = ""
}
