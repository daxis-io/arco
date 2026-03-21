# Gate 5 IAM Prefix Scoping
#
# CRITICAL: Use supported IAM CEL functions. For GCS object paths that means
# extracting the tenant/workspace-relative object path and checking it with
# `startsWith()`. Do NOT use `contains()` for prefix scoping.
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
  # Extract the path segment after tenant/workspace so prefix checks stay anchored.
  object_path_extract_template = "${local.bucket_objects_prefix}tenant={tenant}/workspace={workspace}/{object_path}"

  ledger_object_prefix      = "ledger/"
  locks_object_prefix       = "locks/"
  commits_object_prefix     = "commits/"
  manifests_object_prefix   = "manifests/"
  snapshots_object_prefix   = "snapshots/"
  state_object_prefix       = "state/"
  anti_entropy_state_prefix = "state/anti_entropy/"
  l0_object_prefix          = "l0/"
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
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.ledger_object_prefix}")
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
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.locks_object_prefix}")
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
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.commits_object_prefix}")
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
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.ledger_object_prefix}")
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
      resource.type == "storage.googleapis.com/Object" &&
      (
        resource.name.extract("${local.object_path_extract_template}").startsWith("${local.ledger_object_prefix}") ||
        resource.name.extract("${local.object_path_extract_template}").startsWith("${local.state_object_prefix}")
      )
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
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.snapshots_object_prefix}")
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
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.state_object_prefix}")
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
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.l0_object_prefix}")
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
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.manifests_object_prefix}")
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
      resource.type == "storage.googleapis.com/Object" &&
      (
        resource.name.extract("${local.object_path_extract_template}").startsWith("${local.ledger_object_prefix}") ||
        resource.name.extract("${local.object_path_extract_template}").startsWith("${local.manifests_object_prefix}") ||
        resource.name.extract("${local.object_path_extract_template}").startsWith("${local.snapshots_object_prefix}") ||
        resource.name.extract("${local.object_path_extract_template}").startsWith("${local.state_object_prefix}") ||
        resource.name.extract("${local.object_path_extract_template}").startsWith("${local.l0_object_prefix}")
      )
    EOT
  }
}

# ============================================================================
# Compactor Anti-Entropy Service Account (Patch 9)
# ============================================================================
#
# Anti-entropy job should be the only component that lists objects.
# Cloud Storage evaluates `storage.objects.list` conditions against the bucket,
# not individual objects, so allow-policy conditions cannot safely enforce a
# ledger-only list scope here. We therefore grant list at the bucket level to
# the dedicated anti-entropy service account only.
#
# Permissions: bucket list + read all

# Anti-entropy can list bucket objects to discover missed notifications.
resource "google_storage_bucket_iam_member" "compactor_antientropy_list_bucket" {
  bucket = google_storage_bucket.catalog.name
  role   = google_project_iam_custom_role.storage_object_lister.name
  member = "serviceAccount:${google_service_account.compactor_antientropy.email}"
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
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.anti_entropy_state_prefix}")
    EOT
  }
}
