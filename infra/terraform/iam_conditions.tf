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
#   tenant={tenant}/workspace={workspace}/state-store/ <- API writes (object-store control store; sole writer)

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
  warehouse_object_prefix   = "warehouse/"

  # Object-store control store (ControlMvpPaths::base_prefix() in
  # crates/arco-catalog/src/state_store/control_mvp.rs writes
  # state-store/control-mvp/{domain}/... under the tenant/workspace root).
  # NOTE: startsWith("state/") does NOT match "state-store/" (the 6th character
  # differs), so this prefix needs its own binding and the compactor's state/
  # conditions intentionally never cover it.
  state_store_object_prefix = "state-store/"
}

# ============================================================================
# API Service Account: ledger/, locks/, commits/, warehouse/, state-store/ (read all)
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

# API can create Delta log objects for coordinated commits under managed table roots.
resource "google_storage_bucket_iam_member" "api_write_warehouse_delta" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.api.email}"

  condition {
    title       = "ApiWriteWarehouseDelta"
    description = "API can create Delta log objects under managed warehouse table roots"
    expression  = <<-EOT
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.warehouse_object_prefix}")
    EOT
  }
}

# API is the SOLE writer of the object-store control store under state-store/.
#
# Single-writer invariant (2026-07-30 program audit, sections 5.4 and 9.2 item
# 8): control-store commits happen in-process in the arco-api service
# (ControlMvpStateStore/ControlMvpTxn in
# crates/arco-catalog/src/state_store/control_mvp.rs), so the API service
# account is the only service account with write authority under state-store/.
# The publish protocol creates immutable txlog/, manifests/, and checkpoints/
# objects and then CAS-overwrites current.pointer.json with a
# generation-matched precondition; the pointer overwrite requires
# storage.objects.delete in addition to create, hence objectUser rather than
# objectCreator (same reasoning as api_write_locks above).
#
# Do NOT grant any other service account a condition matching state-store/.
# The compactor's startsWith("state/") conditions do not match "state-store/",
# and tools/xtask/tests/terraform_iam.rs enforces that exactly one binding
# references this prefix. If a control-store compactor/GC ever needs to clean
# orphan state-store artifacts, that authority must be granted deliberately
# with a new single-writer decision, not by widening an existing prefix.
resource "google_storage_bucket_iam_member" "api_write_state_store" {
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.api.email}"

  condition {
    title       = "ApiWriteStateStore"
    description = "Sole writer: API commits object-store control state under state-store/"
    expression  = <<-EOT
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.state_store_object_prefix}")
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

# Flow worker can write produced table data under managed warehouse roots without list access.
resource "google_storage_bucket_iam_member" "flow_worker_write_warehouse" {
  count  = local.flow_services_enabled ? 1 : 0
  bucket = google_storage_bucket.catalog.name
  role   = google_project_iam_custom_role.storage_object_writer_no_list.name
  member = "serviceAccount:${google_service_account.flow_worker[0].email}"

  condition {
    title       = "FlowWorkerWriteWarehouse"
    description = "Flow worker can write produced table data under warehouse/ without list"
    expression  = <<-EOT
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.warehouse_object_prefix}")
    EOT
  }
}

resource "google_storage_bucket_iam_member" "flow_timer_ingest_write_ledger" {
  count  = local.flow_services_enabled ? 1 : 0
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.flow_timer_ingest[0].email}"

  condition {
    title       = "FlowTimerIngestWriteLedger"
    description = "Gate 5: Flow timer ingest can create orchestration ledger events"
    expression  = <<-EOT
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.ledger_object_prefix}")
    EOT
  }
}

resource "google_storage_bucket_iam_member" "flow_timer_ingest_manage_locks" {
  count  = local.flow_services_enabled ? 1 : 0
  bucket = google_storage_bucket.catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.flow_timer_ingest[0].email}"

  condition {
    title       = "FlowTimerIngestManageLocks"
    description = "Gate 5: Flow timer ingest can manage orchestration compaction locks"
    expression  = <<-EOT
      resource.type == "storage.googleapis.com/Object" &&
      resource.name.extract("${local.object_path_extract_template}").startsWith("${local.locks_object_prefix}")
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
# Permissions: bucket list + read all + prefix-scoped cursor writes

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
  role   = google_project_iam_custom_role.storage_object_writer_no_list.name
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
