# Gate 5 IAM Prefix Scoping
#
# CRITICAL: Use `startsWith()` on the full expected prefix or `matches()` with an
# anchored regex. Do NOT use `contains()` for prefix scoping.
#
# Path format: projects/_/buckets/{bucket}/objects/tenant={id}/workspace={id}/{prefix}/...
#
# Why `contains()` is dangerous:
#   resource.name.contains("/state/")  # WRONG
# This matches: tenant=x/ledger/evil/state/bypass.txt <- attacker controls!
#
# Storage Layout (Jan 12 aligned):
#   tenant={tenant}/workspace={workspace}/ledger/     <- API writes (events)
#   tenant={tenant}/workspace={workspace}/locks/      <- API writes (distributed locks)
#   tenant={tenant}/workspace={workspace}/commits/    <- API writes (commit records)
#   tenant={tenant}/workspace={workspace}/manifests/  <- Compactor writes (CAS publish)
#   tenant={tenant}/workspace={workspace}/snapshots/  <- Compactor writes (Tier-1 Parquet)
#   tenant={tenant}/workspace={workspace}/state/      <- Compactor writes (Tier-2 Parquet)
#   tenant={tenant}/workspace={workspace}/l0/         <- Compactor writes (L0 tier)

locals {
  # Base path pattern for all bucket objects
  # GCS object names in IAM conditions are: projects/_/buckets/{bucket}/objects/{path}
  bucket_objects_prefix = "projects/_/buckets/${google_storage_bucket.catalog.name}/objects/"

  # Gate 5 layout: tenant=<id>/workspace=<id>/... with unknown tenant/workspace at deploy time.
  # Use an anchored regex so the protected prefix must occur at the expected path boundary.
  tenant_workspace_prefix_regex = "^${local.bucket_objects_prefix}tenant=[^/]+/workspace=[^/]+/"

  ledger_prefix_regex             = "${local.tenant_workspace_prefix_regex}ledger/"
  locks_prefix_regex              = "${local.tenant_workspace_prefix_regex}locks/"
  commits_prefix_regex            = "${local.tenant_workspace_prefix_regex}commits/"
  manifests_prefix_regex          = "${local.tenant_workspace_prefix_regex}manifests/"
  snapshots_prefix_regex          = "${local.tenant_workspace_prefix_regex}snapshots/"
  state_prefix_regex              = "${local.tenant_workspace_prefix_regex}state/"
  anti_entropy_state_prefix_regex = "${local.tenant_workspace_prefix_regex}state/anti_entropy/"
  l0_prefix_regex                 = "${local.tenant_workspace_prefix_regex}l0/"
}

# ============================================================================
# API Service Account: ledger/, locks/, commits/, manifests/ (read all)
# ============================================================================

# API can create ledger events (immutable, append-only)
resource "google_storage_bucket_iam_member" "api_write_ledger" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.api.email}"

  condition {
    title       = "ApiWriteLedger"
    description = "Gate 5: API can create ledger events (immutable)"
    expression  = <<-EOT
      resource.name.matches("${local.ledger_prefix_regex}")
    EOT
  }
}

# API can manage distributed locks (create, update, delete for lock lifecycle)
resource "google_storage_bucket_iam_member" "api_write_locks" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.api.email}"

  condition {
    title       = "ApiWriteLocks"
    description = "Gate 5: API can manage distributed locks"
    expression  = <<-EOT
      resource.name.matches("${local.locks_prefix_regex}")
    EOT
  }
}

# API can create commit records (immutable audit trail)
resource "google_storage_bucket_iam_member" "api_write_commits" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.api.email}"

  condition {
    title       = "ApiWriteCommits"
    description = "Gate 5: API can create commit records (immutable)"
    expression  = <<-EOT
      resource.name.matches("${local.commits_prefix_regex}")
    EOT
  }
}

# API: Read all objects (no prefix restriction on reads)
resource "google_storage_bucket_iam_member" "api_read_all" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.api.email}"
}

# ============================================================================
# Flow Controller Service Account: ledger/ write + (ledger/, state/) read (NO list)
# ============================================================================

resource "google_storage_bucket_iam_member" "flow_controller_write_ledger" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.flow_controller.email}"

  condition {
    title       = "FlowControllerWriteLedger"
    description = "Gate 5: Flow controllers can create orchestration ledger events"
    expression  = <<-EOT
      resource.name.matches("${local.ledger_prefix_regex}")
    EOT
  }
}

resource "google_storage_bucket_iam_member" "flow_controller_read_objects" {
  bucket = google_storage_bucket.catalog.name
  role   = google_project_iam_custom_role.storage_object_reader_no_list.name
  member = "serviceAccount:${google_service_account.flow_controller.email}"

  condition {
    title       = "FlowControllerReadObjects"
    description = "Gate 5: Flow controllers can read projections (state/) and ledger events (no list)"
    expression  = <<-EOT
      resource.name.matches("${local.ledger_prefix_regex}") ||
      resource.name.matches("${local.state_prefix_regex}")
    EOT
  }
}

# ============================================================================
# Compactor Fast-Path Service Account (Patch 9)
# ============================================================================
#
# CRITICAL: Fast-path compactor has NO list permission.
# This is intentional - listing should only happen in anti-entropy.
# If fast-path code accidentally tries to list, it will fail at runtime.
#
# Permissions: snapshots/, state/, l0/, manifests/ write + read all (NO list)

# Fast-path can write Tier-1 snapshots (Parquet, immutable)
resource "google_storage_bucket_iam_member" "compactor_write_snapshots" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.compactor.email}"

  condition {
    title       = "CompactorFastpathWriteSnapshots"
    description = "Gate 5: Fast-path compactor can write snapshots/ (Tier-1 Parquet)"
    expression  = <<-EOT
      resource.name.matches("${local.snapshots_prefix_regex}")
    EOT
  }
}

# Fast-path can write state files (Parquet snapshots, immutable)
resource "google_storage_bucket_iam_member" "compactor_write_state" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.compactor.email}"

  condition {
    title       = "CompactorFastpathWriteState"
    description = "Gate 5: Fast-path compactor can write state/ (Parquet snapshots)"
    expression  = <<-EOT
      resource.name.matches("${local.state_prefix_regex}")
    EOT
  }
}

# Fast-path can write to l0/ tier (may need cleanup of old files)
resource "google_storage_bucket_iam_member" "compactor_write_l0" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.compactor.email}"

  condition {
    title       = "CompactorFastpathWriteL0"
    description = "Gate 5: Fast-path compactor can write l0/ tier"
    expression  = <<-EOT
      resource.name.matches("${local.l0_prefix_regex}")
    EOT
  }
}

# Fast-path can update manifests (publish compaction results)
resource "google_storage_bucket_iam_member" "compactor_write_manifests" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.compactor.email}"

  condition {
    title       = "CompactorFastpathWriteManifests"
    description = "Gate 5: Fast-path compactor can update manifests"
    expression  = <<-EOT
      resource.name.matches("${local.manifests_prefix_regex}")
    EOT
  }
}

# Fast-path: Read objects without list permission (custom role).
# The SA split + IAM enforces the no-list invariant at runtime.
resource "google_storage_bucket_iam_member" "compactor_fastpath_read_objects" {
  bucket = google_storage_bucket.catalog.name
  role   = google_project_iam_custom_role.storage_object_reader_no_list.name
  member = "serviceAccount:${google_service_account.compactor.email}"

  condition {
    title       = "CompactorFastpathReadObjects"
    description = "Gate 5: Fast-path compactor can read ledger/manifests/state/snapshots/l0"
    expression  = <<-EOT
      resource.name.matches("${local.ledger_prefix_regex}") ||
      resource.name.matches("${local.manifests_prefix_regex}") ||
      resource.name.matches("${local.snapshots_prefix_regex}") ||
      resource.name.matches("${local.state_prefix_regex}") ||
      resource.name.matches("${local.l0_prefix_regex}")
    EOT
  }
}

# ============================================================================
# Compactor Anti-Entropy Service Account (Patch 9)
# ============================================================================
#
# Anti-entropy job can LIST ledger/ to discover missed events.
# This is the ONLY compactor component with list permission.
#
# Permissions: ledger/ list + read all

# Anti-entropy can list AND read ledger events (to discover missed notifications)
resource "google_storage_bucket_iam_member" "compactor_antientropy_list_ledger" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.compactor_antientropy.email}"

  condition {
    title       = "CompactorAntiEntropyListLedger"
    description = "Gate 5: Anti-entropy can list ledger/ to find missed events"
    expression  = <<-EOT
      resource.name.matches("${local.ledger_prefix_regex}")
    EOT
  }
}

# Anti-entropy: Read all objects for state verification
# Anti-entropy: Read all objects without list permission
resource "google_storage_bucket_iam_member" "compactor_antientropy_read_all" {
  bucket = google_storage_bucket.catalog.name
  role   = google_project_iam_custom_role.storage_object_reader_no_list.name
  member = "serviceAccount:${google_service_account.compactor_antientropy.email}"
}

resource "google_storage_bucket_iam_member" "compactor_antientropy_write_cursor" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.compactor_antientropy.email}"

  condition {
    title       = "CompactorAntiEntropyWriteCursor"
    description = "Gate 5: Anti-entropy can update state/anti_entropy cursor"
    expression  = <<-EOT
      resource.name.matches("${local.anti_entropy_state_prefix_regex}")
    EOT
  }
}
