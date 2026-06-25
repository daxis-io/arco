# Arco Terraform Variables
#
# Central variable definitions for the Arco infrastructure.

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "project_number" {
  description = "GCP project number (used for deterministic Cloud Run URLs). If unset, Terraform will try to read it via the Cloud Resource Manager API."
  type        = string
  default     = ""

  validation {
    condition     = var.project_number == "" || can(regex("^\\d+$", var.project_number))
    error_message = "project_number must be digits only (or empty to auto-discover)."
  }
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

variable "api_code_version" {
  description = "Code version stamped on runs (e.g., git SHA or release tag)"
  type        = string
  default     = ""
}

variable "api_git_sha" {
  description = "Git SHA for Arco API build provenance exposed by /version"
  type        = string
  default     = ""
}

variable "deploy_owner" {
  description = "Optional label-safe deployment owner marker stamped on Cloud Run services during managed deploys."
  type        = string
  default     = ""

  validation {
    condition     = var.deploy_owner == "" || can(regex("^[a-z0-9]([-a-z0-9_]{0,61}[a-z0-9])?$", var.deploy_owner))
    error_message = "deploy_owner must be empty or a GCP label-safe value: lowercase letters, digits, '-' or '_', 1-63 chars, starting and ending with a letter or digit."
  }
}

variable "compactor_image" {
  description = "Container image for Arco Compactor service"
  type        = string
}

variable "flow_compactor_image" {
  description = "Container image for Arco Flow compactor service"
  type        = string
  default     = ""
}

variable "flow_compactor_ingress" {
  description = "Ingress mode for the Arco Flow compactor service. Use all only for bounded UAT/debug windows with IAM invoker protection."
  type        = string
  default     = "internal"

  validation {
    condition     = contains(["internal", "all"], var.flow_compactor_ingress)
    error_message = "flow_compactor_ingress must be one of: internal, all."
  }
}

variable "flow_automation_reconciler_image" {
  description = "Container image for Arco Flow automation reconciler service"
  type        = string
  default     = ""
}

variable "flow_dispatcher_image" {
  description = "Container image for Arco Flow dispatcher service"
  type        = string
  default     = ""
}

variable "flow_sweeper_image" {
  description = "Container image for Arco Flow sweeper service"
  type        = string
  default     = ""
}

variable "flow_timer_ingest_image" {
  description = "Container image for Arco Flow timer-ingest service"
  type        = string
  default     = ""
}

variable "flow_worker_image" {
  description = "Container image for Arco Flow worker service"
  type        = string
  default     = ""
}

variable "flow_tenant_id" {
  description = "Tenant ID for flow control-plane services"
  type        = string
  default     = ""
}

variable "flow_workspace_id" {
  description = "Workspace ID for flow control-plane services"
  type        = string
  default     = ""
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
# Task Callback Token Configuration
# ============================================================================

variable "task_token_secret" {
  description = "HS256 secret used by flow dispatchers/sweepers to mint task callback tokens and by the API to validate them"
  type        = string
  default     = ""
  sensitive   = true
}

variable "task_token_issuer" {
  description = "Issuer claim for task callback tokens"
  type        = string
  default     = ""
}

variable "task_token_audience" {
  description = "Audience claim for task callback tokens"
  type        = string
  default     = ""
}

variable "task_token_ttl_secs" {
  description = "Task callback token TTL in seconds"
  type        = number
  default     = 3600
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

variable "background_automation_enabled" {
  description = "Whether scheduled background automation and warm background services stay enabled"
  type        = bool
  default     = true
}

variable "compactor_repair_automation_mode" {
  description = "Catalog compactor repair automation mode"
  type        = string
  default     = "enforce"

  validation {
    condition     = contains(["disabled", "dry_run", "enforce"], var.compactor_repair_automation_mode)
    error_message = "compactor_repair_automation_mode must be one of: disabled, dry_run, enforce."
  }
}

variable "compactor_repair_automation_interval_secs" {
  description = "Catalog compactor repair automation cadence in seconds"
  type        = number
  default     = 300

  validation {
    condition     = var.compactor_repair_automation_interval_secs > 0
    error_message = "compactor_repair_automation_interval_secs must be greater than 0."
  }
}

variable "compactor_repair_automation_scope" {
  description = "Catalog compactor repair automation scope"
  type        = string
  default     = "full"

  validation {
    condition     = contains(["current_head_only", "full"], var.compactor_repair_automation_scope)
    error_message = "compactor_repair_automation_scope must be one of: current_head_only, full."
  }
}

variable "compactor_repair_automation_domains" {
  description = "Comma-separated catalog domains to target with repair automation"
  type        = string
  default     = "catalog,lineage,search"
}

variable "flow_compactor_repair_automation_mode" {
  description = "Orchestration compactor repair automation mode"
  type        = string
  default     = "enforce"

  validation {
    condition     = contains(["disabled", "dry_run", "enforce"], var.flow_compactor_repair_automation_mode)
    error_message = "flow_compactor_repair_automation_mode must be one of: disabled, dry_run, enforce."
  }
}

variable "flow_compactor_repair_automation_interval_secs" {
  description = "Orchestration compactor repair automation cadence in seconds"
  type        = number
  default     = 300

  validation {
    condition     = var.flow_compactor_repair_automation_interval_secs > 0
    error_message = "flow_compactor_repair_automation_interval_secs must be greater than 0."
  }
}

variable "flow_compactor_repair_automation_scope" {
  description = "Orchestration compactor repair automation scope"
  type        = string
  default     = "full"

  validation {
    condition     = contains(["current_head_only", "full"], var.flow_compactor_repair_automation_scope)
    error_message = "flow_compactor_repair_automation_scope must be one of: current_head_only, full."
  }
}

variable "flow_automation_reconciler_schedule" {
  description = "Cron schedule for the flow automation reconciler /run trigger"
  type        = string
  default     = "*/1 * * * *"
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

variable "flow_cpu" {
  description = "CPU allocation for Flow services"
  type        = string
  default     = "1"
}

variable "flow_memory" {
  description = "Memory allocation for Flow services"
  type        = string
  default     = "512Mi"
}

variable "flow_min_instances" {
  description = "Minimum instances for Flow services"
  type        = number
  default     = 0
}

variable "flow_max_instances" {
  description = "Maximum instances for Flow services"
  type        = number
  default     = 2
}

variable "flow_require_tasks_oidc" {
  description = "Require Cloud Tasks OIDC for flow dispatcher/sweeper"
  type        = bool
  default     = true
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

variable "tenant_secret_name" {
  description = "Secret Manager secret name containing tenant secret (base64) for orchestration run_id HMAC"
  type        = string
  default     = "arco-tenant-secret"
}

variable "flow_worker_dispatch_secret_name" {
  description = "Secret Manager secret name containing the shared worker dispatch secret"
  type        = string
  default     = "arco-flow-worker-dispatch-secret"
}

# ============================================================================
# Networking
# ============================================================================

variable "api_public" {
  description = "Whether API is publicly accessible (false = internal only)"
  type        = bool
  default     = false
}

variable "compactor_ingress" {
  description = "Cloud Run ingress setting for the compactor service"
  type        = string
  default     = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  validation {
    condition = contains([
      "INGRESS_TRAFFIC_ALL",
      "INGRESS_TRAFFIC_INTERNAL_ONLY",
      "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER",
    ], var.compactor_ingress)
    error_message = "compactor_ingress must be a valid Cloud Run v2 ingress enum."
  }
}

variable "vpc_connector_name" {
  description = "Serverless VPC connector name (optional, for private services)"
  type        = string
  default     = ""
}

# ============================================================================
# Cloud Tasks (Flow Dispatcher / Timer Callbacks)
# ============================================================================

variable "flow_dispatch_queue_name" {
  description = "Cloud Tasks queue name for task dispatch payloads"
  type        = string
  default     = "arco-flow-dispatch"
}

variable "flow_timer_queue_name" {
  description = "Cloud Tasks queue name for timer callback payloads"
  type        = string
  default     = "arco-flow-timers"
}

variable "flow_dispatcher_service_name" {
  description = "Optional Cloud Run service name for arco_flow_dispatcher (used for timer callback invoker IAM)"
  type        = string
  default     = ""
}

variable "flow_queue_max_attempts" {
  description = "Maximum Cloud Tasks delivery attempts for flow queues"
  type        = number
  default     = 5
}

variable "flow_queue_min_backoff_seconds" {
  description = "Minimum Cloud Tasks retry backoff for flow queues"
  type        = number
  default     = 10
}

variable "flow_queue_max_backoff_seconds" {
  description = "Maximum Cloud Tasks retry backoff for flow queues"
  type        = number
  default     = 300
}

variable "flow_queue_max_retry_duration_seconds" {
  description = "Maximum retry window for flow queues"
  type        = number
  default     = 3600
}

variable "flow_queue_max_dispatches_per_second" {
  description = "Max dispatch throughput per second for flow queues"
  type        = number
  default     = 50
}

variable "flow_queue_max_concurrent_dispatches" {
  description = "Max concurrent dispatches for flow queues"
  type        = number
  default     = 500
}
