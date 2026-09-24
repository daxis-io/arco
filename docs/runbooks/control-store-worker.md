# Runbook: Control-Store Worker Job

Binary: `crates/arco-api/src/bin/arco_control_store_worker.rs`
Terraform: `infra/terraform/cloud_run_job.tf` (`google_cloud_run_v2_job.control_store_worker`)
Cloud Run job: `arco-control-store-worker-<env>` (scheduler trigger
`arco-control-store-worker-trigger-<env>`, default cadence `*/5 * * * *`)

## What it does

One invocation is single-shot and runs three phases against the `control/v1`
root selected by the configured tenant and workspace. Phases are independent:
a failure in one is logged and recorded, and the remaining phases still run. The
process exits `0` only when every phase completed or deferred to the next run,
and non-zero when any phase failed with a typed error, so a failed execution is
visible in Cloud Run job history and in Cloud Scheduler retries.

1. **Catalog projection drain** (`phase="drain"`, `domain="catalog"`).
   `CatalogProjectionMaterializer::drain_once` materializes every pending
   catalog projection outbox record and acknowledges it, then reads the durable
   materialization status and logs applied/observed sequence, lag and age.
   This is the restart-safe anti-entropy path behind the fail-open post-commit
   drain the API spawns.
2. **Layout maintenance** (`phase="maintenance"`), for each control domain in
   order: `catalog`, `projection-outbox-acks`. When the current manifest
   selects a maintenance intent (for example after 16 unconsolidated L0
   segments), the worker prepares, starts, advances and publishes one durable
   maintenance job through `DurableMaintenanceWorker`. Commits refuse at 32
   unconsolidated L0 segments, so a domain that never sees this phase wedges.
3. **Garbage collection** (`phase="gc"`), for each control domain: up to
   `ARCO_CONTROL_STORE_GC_MAX_PAGES` pages of `collect_gc_page_at`, the
   conservative active collector (unreachable candidates older than seven days,
   pins retained 30 days).

The job runs under the **API service account**. That account is the sole
writer of the `control/` prefix; do not rebind the job to a compactor service
account.

## Environment variables

| Variable | Required | Meaning |
| --- | --- | --- |
| `ARCO_STORAGE_BUCKET` | yes | Catalog bucket (Terraform wires `google_storage_bucket.catalog`). |
| `ARCO_CATALOG_CONTROL_V1_TENANT_ID` | yes | Tenant of the control root. Must equal the API's value. |
| `ARCO_CATALOG_CONTROL_V1_WORKSPACE_ID` | yes | Workspace of the control root. Must equal the API's value. |
| `ARCO_CONTROL_STORE_MAINTENANCE_BINDING` | yes | Standard base64 of exactly 32 bytes; see the binding rule below. Mounted from Secret Manager (`control_store_maintenance_binding_secret`). |
| `ARCO_CONTROL_STORE_GC_MAX_PAGES` | no (default 16) | GC pages collected per domain per run. |
| `ARCO_CONTROL_STORE_MAINTENANCE_MAX_ADVANCES` | no (default 4096) | `advance_at` calls per maintenance job per run; the job resumes next run when exhausted. |
| `ARCO_LOG_FORMAT` | no | `json` for structured logs (Terraform sets it); anything else is pretty. |
| `RUST_LOG` | no | Standard `tracing` filter; defaults to `info`. |

Terraform enables the job only when `control_store_worker_image`,
`control_store_tenant_id`, `control_store_workspace_id` and
`control_store_maintenance_binding_secret` are all non-empty. `scripts/deploy.sh`
accepts the same values as `CONTROL_STORE_WORKER_IMAGE`,
`CONTROL_STORE_TENANT_ID`, `CONTROL_STORE_WORKSPACE_ID` and
`CONTROL_STORE_MAINTENANCE_BINDING_SECRET` and refuses a partial set. The
scheduler trigger is additionally gated on `background_automation_enabled`.

## The binding secret must never change for a live root

`ARCO_CONTROL_STORE_MAINTENANCE_BINDING` is the `DurableAuthorityBinding` for
this deployment's control root. The maintenance kernel writes it into every
durable job descriptor and rejects descriptors whose binding differs from the
worker's own, so it must:

- be generated once per control root (per provider location, bucket and
  tenant/workspace), for example `head -c 32 /dev/urandom | base64`;
- stay identical across replicas, credentials, restarts and image upgrades;
- change only when the root itself changes (a new bucket or a new
  tenant/workspace root), never as a routine rotation;
- never be derived from a persisted job or a process-local state-store binding.

Rotating it on a live root orphans every in-flight maintenance job: the next
run cannot resume them, and their retention pins stay in place until the
descriptor lifetime (24 h) and pin retention (8 days) expire. If a rotation has
already happened, restore the previous secret version
(`gcloud secrets versions access <previous> --secret <id>`) and re-run the job.

## Running it manually

```bash
# Execute one run now and wait for it to finish.
gcloud run jobs execute "arco-control-store-worker-${ENV}" \
  --project "${PROJECT_ID}" --region "${REGION}" --wait

# Bound a one-off run (for example after a large backlog) without changing Terraform.
gcloud run jobs execute "arco-control-store-worker-${ENV}" \
  --project "${PROJECT_ID}" --region "${REGION}" --wait \
  --update-env-vars ARCO_CONTROL_STORE_GC_MAX_PAGES=64,ARCO_CONTROL_STORE_MAINTENANCE_MAX_ADVANCES=8192

# Inspect executions.
gcloud run jobs executions list --job "arco-control-store-worker-${ENV}" \
  --project "${PROJECT_ID}" --region "${REGION}"
```

The image is built with
`scripts/build-cloud-run-image.sh --bin arco_control_store_worker --image <tag>`
and needs the `gcp` feature (`--features gcp`) for GCS access, like the other
Cloud Run images.

Locally, the same binary runs against any bucket `arco_storage::from_bucket`
accepts:

```bash
ARCO_STORAGE_BUCKET=gs://my-bucket \
ARCO_CATALOG_CONTROL_V1_TENANT_ID=tenant \
ARCO_CATALOG_CONTROL_V1_WORKSPACE_ID=workspace \
ARCO_CONTROL_STORE_MAINTENANCE_BINDING="$(gcloud secrets versions access latest --secret "$SECRET")" \
cargo run -p arco-api --features gcp --bin arco_control_store_worker
```

## Reading the summary logs

Every phase emits one structured line with `message="control-store worker
phase complete"` (or `"... phase failed"`), and the run ends with
`message="control-store worker run complete"`. Filter in Logs Explorer with
`resource.type="cloud_run_job" AND jsonPayload.fields.phase="maintenance"` (the
exact field path depends on how the JSON layer nests `tracing` fields).

| Phase | Key fields |
| --- | --- |
| `drain` | `drained_records`, `quarantined_records`, `already_acknowledged`, `latest_projected_sequence`, `applied_authority_sequence`, `observed_authority_sequence`, `lag` (observed - applied), `age_secs` (since last successful materialization), `elapsed_ms` |
| `maintenance` | `domain`, `outcome`, `job_id`, `advances`, `completed`, `total`, `layout_generation`, `elapsed_ms` |
| `gc` | `domain`, `pages`, `objects_deleted`, `bytes_reclaimed`, `truncated`, `elapsed_ms` |
| `run` | `outcome` (`ok`/`error`), `failures`, `elapsed_ms` |

Maintenance `outcome` values:

- `idle`: the manifest selects no maintenance intent. Normal steady state.
- `published`: a consolidated layout was published by exact head CAS;
  `layout_generation` is the new generation.
- `deferred`: another actor holds or consumed the source (a
  `PreconditionFailed`/`CasFailed` from prepare/start/resume/advance/publish, or
  a publication consumed by a different publication). The run still exits `0`
  and the next run retries with a fresh plan. Repeated `deferred` on the same
  domain across several runs means something else is publishing that root
  (check the API's request-path maintenance and any manual operator jobs).
- `terminal`: the job reached `Failed`, `Superseded` or `Abandoned`. The next
  run prepares a fresh plan; investigate if it repeats.
- `exhausted`: the advance budget ran out before `ReadyToPublish`. Raise
  `ARCO_CONTROL_STORE_MAINTENANCE_MAX_ADVANCES` or run the job again; the job
  resumes from durable progress.

A `phase failed` line with `outcome="error"` carries the typed error and makes
the execution exit non-zero. Storage, integrity, ambiguous-outcome and
backpressure errors are never retried silently; see
`docs/runbooks/state-store-cas-publish-failure.md` and
`docs/runbooks/state-store-corrupt-artifact.md`.

## Known limitations

- **GC cursor is not persisted.** Each run starts GC from the beginning of the
  domain prefix and collects at most `ARCO_CONTROL_STORE_GC_MAX_PAGES` pages
  (`truncated=true` in the summary when a continuation remained). A domain whose
  candidate inventory exceeds the page budget is re-listed from the start every
  run; the cost is repeated listing, not lost deletions, because every eligible
  object is reconsidered each run. Raise the page budget for one-off backlogs.
- **Maintenance job identity is not persisted outside the kernel.** The worker
  holds the prepared job id in memory for the duration of one run. If the
  process dies between `start_at` and `publish_at`, the kernel's durable
  progress and retention pin remain; the next run prepares a new plan and the
  orphaned job expires with its descriptor lifetime.
- **Cadence does not meet ADR-043's projection-lag objective.** ADR-043 requires
  projection p99 lag of at most 10 seconds with no normal interval above 60
  seconds. A 5-minute cron trigger bounds worst-case drain latency at the
  schedule period, so this job is the anti-entropy backstop behind the API's
  fail-open post-commit drain, not the lag-objective mechanism. Queue-driven
  wake (a commit-fired trigger) remains cutover work; do not tighten the cron
  below what the run duration allows, because overlapping executions contend
  on the workspace retention lock.
- The job maintains exactly the `catalog` and `projection-outbox-acks` domains.
  Other control domains need their own entry in `CONTROL_DOMAINS`.

## Related

- `docs/runbooks/state-store-projection-lag.md`
- `docs/runbooks/state-store-replay-budget.md`
- `docs/runbooks/gc-failure.md`
- `docs/adr/adr-043-s3-state-token-authority.md`
