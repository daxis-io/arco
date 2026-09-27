# Runbook: Control-Store Worker Job

Binary: `crates/arco-api/src/bin/arco_control_store_worker.rs`
Terraform: `infra/terraform/cloud_run_job.tf` (`google_cloud_run_v2_job.control_store_worker`)
Cloud Run job: `arco-control-store-worker-<env>` (scheduler trigger
`arco-control-store-worker-trigger-<env>`, default cadence `*/5 * * * *`,
task timeout 1800 s, no Cloud Run task retries)

## What it does

One invocation is single-shot and runs five phases against the `control/v1`
root selected by the configured tenant and workspace. Phases are independent:
a failure in one is logged and recorded, and the remaining phases still run. The
process exits `0` only when every phase completed or deferred to the next run,
and non-zero when any phase failed with a typed error or the workspace
retention epoch is stuck, so a failed execution is visible in Cloud Run job
history.

1. **Retention epoch inspection** (`phase="epoch"`, `domain="workspace"`).
   Reads `retention/coordination/mutation-epoch.json` and classifies it (see
   `stuck_epoch` below). This is the backstop for the one failure mode the
   worker cannot heal on its own.
2. **Layout maintenance** (`phase="maintenance"`), for each control domain in
   order: `catalog`, `projection-outbox-acks`. A domain gets at most two jobs
   per run, a consolidation (`kind="consolidation"`) and then a retention
   horizon (`kind="retention_horizon"`); each job logs its own summary line.
   - *Consolidation.* When the current manifest selects a maintenance intent
     (after 16 unconsolidated L0 segments), the worker prepares a durable
     maintenance job, **persists its identity** at
     `locks/control-store-worker/<domain>/selected-job.json`, then starts,
     advances and publishes it through `DurableMaintenanceWorker`. Commits
     refuse at 32 unconsolidated L0 segments, so a domain that never sees
     this phase wedges.
   - *Retention horizon.* Once the consolidation slot ended `idle` or
     `published`, the worker prepares a `RetentionHorizon` job
     (`prepare_horizon_at`) over the consolidated head and drives it through
     the same persisted-identity path. The kernel computes the horizon from
     three inputs: the age bound (the newest manifest stamped at or before
     `now - 30 d - 1 h`, the token-retention floor plus a one-hour clock-skew
     margin, found by following `age_anchor` links from the head), the
     active snapshot and export pins naming the scope, and the checkpoints
     still inside their retention; the horizon is the minimum of them. The
     job purges only rows whose `expires_at_ms` is older than `now - 1 h`
     and tombstones at or below the horizon; live rows and outbox rows are
     never purged. It admits no plan (`idle`) when no row is purge-eligible.
     Before the head CAS the kernel recomputes the purge over a fresh replay
     of the parent and refuses with `PreconditionFailed` when a later commit
     rewrote a purged key; the worker reports that as `deferred` (see
     "Known limitations").
   - *Recovery.* If a persisted identity already exists from an earlier run,
     the worker resumes that exact job first, whatever its kind (`resume_at`
     → advance → publish, which also finishes a job whose publication already
     reached HEAD before the record was cleared) and only replays activation
     (`recover_activation_at` → `resume_at`) when the job is not directly
     resumable, for example because its selector never landed. It never
     prepares a new plan while a record exists. The replayed job fills this
     run's slot for its kind: after a replayed consolidation that `published`
     the horizon runs; after a replayed horizon a fresh consolidation runs and
     no second horizon. A `deferred` or `exhausted` job keeps its record and
     ends the domain's phase for this run; a `terminal` consolidation clears
     its record but withholds the horizon until the next run. The record
     carries an optional `kind`; a record written before the field existed
     is a consolidation.
3. **Catalog projection drain** (`phase="drain"`, `domain="catalog"`).
   `CatalogProjectionMaterializer::drain_once` materializes every pending
   catalog projection outbox record and acknowledges it, then reads the durable
   materialization status and backlog and logs applied/observed sequence, lag,
   age and pending records. Every drained record commits two
   `projection-outbox-acks` L0 segments (status + ack), which is why
   maintenance runs first: it consolidates the ack domain before the drain
   needs the commit headroom.
4. **Catalog outbox trim** (`phase="trim"`, `domain="catalog"`). After the
   drain, whatever its outcome, `CatalogProjectionMaterializer::trim_once`
   removes the records the catalog consumer has acknowledged from the catalog
   projection outbox. Ordering contract: the kernel first retires this
   consumer's acknowledgements in the `projection-outbox-acks` root, then
   commits an exact-incarnation trim in the catalog root under the
   cooperative writer epoch, so no acknowledgement outlives its record and an
   interrupted pass redelivers rather than loses. The trim goes through the
   fixed-consumer path (`ProjectionOutboxWorker::trim_fixed_consumer`), which
   never installs the generic trim-binding key in the catalog root; the
   operator endpoint keeps refusing catalog trims. A trim commit is one
   catalog L0 segment and advances the catalog logical sequence without a
   projection intent.
5. **Garbage collection** (`phase="gc"`), for each control domain: up to
   `ARCO_CONTROL_STORE_GC_MAX_PAGES` pages of `collect_gc_page_at`, the
   conservative active collector (unreachable candidates older than seven days;
   maintenance retention pins last eight days, token/checkpoint pins 30 days,
   manifests 30 days plus a one-hour clock-skew margin).

The job runs under the **API service account**. That account is the sole
writer of the `control/` prefix; do not rebind the job to a compactor service
account. The persisted job identity lives under `locks/` (an API-writable
prefix) on purpose: `control/` is the kernel's namespace and its GC treats
foreign objects there as orphans.

## Environment variables

| Variable | Required | Meaning |
| --- | --- | --- |
| `ARCO_STORAGE_BUCKET` | yes | Catalog bucket (Terraform wires `google_storage_bucket.catalog`). |
| `ARCO_CATALOG_CONTROL_V1_TENANT_ID` | yes | Tenant of the control root. Must equal the API's value. |
| `ARCO_CATALOG_CONTROL_V1_WORKSPACE_ID` | yes | Workspace of the control root. Must equal the API's value. |
| `ARCO_CONTROL_STORE_MAINTENANCE_BINDING` | yes | Standard base64 of exactly 32 bytes; see the binding rule below. Mounted from Secret Manager (`control_store_maintenance_binding_secret`). |
| `ARCO_CONTROL_STORE_GC_MAX_PAGES` | no (default 16) | GC pages collected per domain per run. |
| `ARCO_CONTROL_STORE_MAINTENANCE_MAX_ADVANCES` | no (default 4096) | `advance_at` calls per maintenance job per run; the job resumes from its persisted identity next run when exhausted. |
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

Rotating it on a live root orphans every in-flight maintenance job: the
persisted identity can no longer be replayed (`recover_activation_at` rejects
the mismatched binding), and their retention pins stay in place until the
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

Do not run two executions concurrently: they contend on the workspace
retention lock and the second one's phases will all report `deferred`.

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
| `epoch` | `outcome`, `epoch`, `operation_kind`, `operation_id`, `holder_id`, `in_flight_for_secs`, `elapsed_ms` |
| `maintenance` | `domain`, `kind`, `outcome`, `job_id`, `recovered`, `advances`, `completed`, `total`, `layout_generation`, `purged_expired_rows`, `purged_tombstones`, `elapsed_ms` |
| `drain` | `outcome`, `drained_records`, `quarantined_records`, `already_acknowledged`, `pending_records`, `latest_projected_sequence`, `applied_authority_sequence`, `observed_authority_sequence`, `lag` (observed - applied), `age_secs` (since last successful materialization), `elapsed_ms` |
| `trim` | `outcome`, `trimmed_records`, `trim_sequence` (catalog logical sequence of the trim commit, when one landed), `elapsed_ms` |
| `gc` | `domain`, `pages`, `objects_deleted`, `bytes_reclaimed`, `truncated`, `elapsed_ms` |
| `run` | `outcome` (`ok`/`error`), `failures`, `elapsed_ms` |

Epoch `outcome` values:

- `absent` / `idle`: nothing in flight. Normal.
- `in_flight`: an epoch is in flight but younger than
  `STALE_RECLAMATION_EPOCH_MIN_AGE_SECS` (600 s), or it is a `control_gc`
  epoch, which the kernel adopts on its own once stale. Maintenance and GC in
  this run report `deferred`; the next run normally proceeds.
- `recoverable`: a stale `maintenance_root_publish` epoch whose `operation_id`
  matches a persisted job identity. The maintenance phase of the same run
  replays that job (`recovered=true`), which settles the epoch.
- `stuck_epoch`: a stale, non-`control_gc` epoch that no persisted job can
  replay (its holder died between claiming and settling, and the identity was
  lost or belongs to another operation). **The run exits non-zero.** Every
  maintenance activation and GC page in the workspace fails closed with
  "a retention mutation epoch is already in flight" until an operator settles
  it; see "Stuck retention epoch" below.

Maintenance `kind` values (one summary line per job, at most two per domain
per run):

- `consolidation`: folds the L0 suffix into an equivalent L1 anchor.
  `purged_expired_rows` and `purged_tombstones` are absent.
- `retention_horizon`: renders L1 without expired rows and unobservable
  tombstones. A `published` entry reports `purged_expired_rows` and
  `purged_tombstones` from the certificate; both are absent unless the job
  published.

Maintenance `outcome` values:

- `idle`: the kernel admitted no plan: the manifest selects no maintenance
  intent (`consolidation`) or no row is purge-eligible (`retention_horizon`).
  Normal steady state.
- `published`: a layout was published by exact head CAS;
  `layout_generation` is the new generation. `recovered=true` means the job
  was replayed from an earlier run's persisted identity.
- `deferred`: another actor holds or consumed the source (a
  `PreconditionFailed`/`CasFailed` from recover/prepare/start/resume/advance/
  publish, a publication consumed by a different publication, or a
  `retention_horizon` whose purged set a later commit superseded). The run
  still exits `0`; a persisted identity stays in place and the next run
  replays it. Repeated `deferred` on the same domain across several runs means
  one of: the `epoch` phase reports `in_flight`/`stuck_epoch` (fix the epoch
  first), a manual `gcloud run jobs execute` overlapping the scheduled run, or
  a restore/snapshot workflow holding the workspace retention lock.
- `terminal`: the job reached `Failed`, `Superseded` or `Abandoned`; its
  persisted identity is cleared and the next run prepares a fresh plan.
  Investigate if it repeats.
- `exhausted`: the advance budget ran out before `ReadyToPublish`. Raise
  `ARCO_CONTROL_STORE_MAINTENANCE_MAX_ADVANCES` or run the job again; the job
  resumes from durable progress via its persisted identity.

Drain `outcome` values:

- `ok`: every pending record was materialized and acknowledged.
- `deferred`: the ack domain hit commit backpressure (32 L0 segments) mid-drain.
  Already-acknowledged records are durable; the next run's maintenance phase
  consolidates the ack domain and the drain continues. A backlog of `N`
  records needs roughly `N / 15` runs. `pending_records` shows the remaining
  backlog; if it does not fall between runs, the maintenance phase for
  `projection-outbox-acks` is not publishing (check its `outcome`).

Trim `outcome` values:

- `ok`: a trim commit landed; `trimmed_records` records left the catalog
  outbox and `trim_sequence` is the commit's catalog logical sequence.
- `idle`: no acknowledged record remained in the outbox; nothing was
  committed and the catalog sequence is unchanged.
- `deferred`: catalog commit backpressure (`MaintenanceBackpressure`),
  `PreconditionFailed` or `CasFailed` stopped the trim; logged at warn with
  the error. The acknowledgements were already retired in the ack root
  before the catalog commit, so the next drain re-materializes those records
  (at-least-once) and the following trim removes them.

A `phase failed` line with `outcome="error"` carries the typed error and makes
the execution exit non-zero. Maintenance failures name the job kind, for
example `maintenance[catalog]: maintenance prepare (retention_horizon) for
domain catalog: ...`; entries already finished in the same domain survive a
later step's error. Storage, integrity, ambiguous-outcome and
backpressure (outside the drain) errors are never retried silently; see
`docs/runbooks/state-store-cas-publish-failure.md` and
`docs/runbooks/state-store-corrupt-artifact.md`.

## Metrics

The kernel emits these while the worker drives it
(`crates/arco-catalog/src/metrics.rs`):

- `arco_state_store_maintenance_published_total{domain,kind}`: durable
  maintenance publications; `kind` is `consolidation` or
  `retention_horizon`. Counted at most once per publication, by the
  invocation whose progress-selector CAS advanced the job to `Published`; a
  publication whose selector PUT persisted but reported an error is never
  counted, so the series can undercount but never overcount.
- `arco_state_store_retention_purged_rows_total{domain,reason}`: rows purged
  by published `retention_horizon` certificates; `reason` is `expired` or
  `tombstone`. Same at-most-once accounting; a zero count emits nothing.
- `arco_control_store_outbox_trimmed_records_total{domain,consumer}`:
  acknowledged outbox records removed by trim commits, emitted after the
  source trim commit succeeds; a zero count emits nothing.

## Stuck retention epoch

Symptom: `phase="epoch"` reports `outcome="stuck_epoch"` and the run exits
non-zero; every maintenance and GC phase reports `deferred`.

Cause: a process died (or a PUT outcome stayed uncertain) between claiming the
workspace retention mutation epoch and settling it. The kernel adopts only
stale `control_gc` epochs automatically; publication epochs such as
`maintenance_root_publish` fail closed by design.

Remedy, in order:

1. Confirm the holder is dead: `holder_id` in the log names the lock holder;
   check Cloud Run executions for the worker and any snapshot/export/restore
   operations for the workspace. Never settle an epoch whose holder may still
   be running.
2. If `operation_kind="maintenance_root_publish"`, check whether
   `locks/control-store-worker/<domain>/selected-job.json` exists for any
   domain with `job_id` equal to the epoch's `operation_id`. If it does, the
   worker reports `recoverable`, not `stuck_epoch`; simply re-run the job.
3. Otherwise settle it with the operator override
   `arco_catalog::recover_stale_retention_epoch(&storage, "<reason>")` (see
   `docs/runbooks/control-plane-repair-and-dark-launch.md` for the invocation
   pattern). It acquires the retention lease first, so it refuses while a live
   holder still owns it, and logs `arco_retention_epoch_recovered_total` with
   the discarded holder identity.
4. Re-run the job; the next `epoch` line must report `idle`.

## Known limitations

- **GC cursor is not persisted.** Each run starts GC from the beginning of the
  domain prefix and collects at most `ARCO_CONTROL_STORE_GC_MAX_PAGES` pages
  (`truncated=true` in the summary when a continuation remained). A domain whose
  candidate inventory exceeds the page budget is re-listed from the start every
  run; the cost is repeated listing, not lost deletions, because every eligible
  object is reconsidered each run. Raise the page budget for one-off backlogs.
- **Persisted job identity is per domain, not per attempt.** A record older
  than the kernel's 24 h descriptor lifetime can be root-recovered but never
  resumed or published; the worker recovers it, clears the record and prepares
  a fresh plan. A record whose descriptor never landed (kill between the record
  write and the first descriptor PUT) is cleared the same way. If an expired
  record cannot even be root-recovered, the worker abandons it and the epoch it
  held surfaces as `stuck_epoch` on the next run rather than staying silent.
- **Cadence does not meet ADR-043's projection-lag objective.** ADR-043 requires
  projection p99 lag of at most 10 seconds with no normal interval above 60
  seconds. A 5-minute cron trigger bounds worst-case drain latency at the
  schedule period, so this job is the anti-entropy backstop behind the API's
  fail-open post-commit drain, not the lag-objective mechanism. Queue-driven
  wake (a commit-fired trigger) remains cutover work; do not tighten the cron
  below what the run duration allows, because overlapping executions contend
  on the workspace retention lock.
- **Each trimming run adds one catalog L0 segment.** A trim commit is a
  catalog transaction, folded by the next consolidation. A catalog outbox
  trimmed on every run accrues one segment per run toward the 16-segment
  maintenance intent even with no application writes.
- **A trim reads as watermark lag 1 and stale freshness until the next
  catalog mutation.** The trim commit advances the catalog logical sequence
  without a projection intent, so from the next run
  `arco_state_store_projection_watermark_lag_sequences` (committed catalog
  sequence minus latest acknowledged) reads 1 and the drain summary's
  `age_secs` keeps growing, because an empty drain materializes nothing and
  rewrites no status, until a catalog mutation is materialized.
  `ArcoProjectionWatermarkLagHigh` fires at 1000 and is unaffected;
  `pending_records` is the truthful backlog signal.
- **A horizon superseded by a later commit stays `deferred` until its record
  expires.** When a commit after preparation rewrote a purged key,
  `publish_at` refuses with `PreconditionFailed` and leaves the job's status
  unchanged; the worker reports `deferred`, keeps the record, and every
  following run replays the same job into the same refusal. The worker
  prepares a fresh plan only once the record is older than the 24 h
  descriptor lifetime (root recovery then clears it), and nothing else runs
  in that domain's maintenance phase meanwhile, consolidation included. It
  has no path to abandon the job earlier (`DurableMaintenanceWorker::abandon_at`
  is the kernel operation).
- **Writer clocks more than one hour behind object-store time defeat the
  skew margin.** The horizon's purge cutoff and age bound subtract a
  one-hour clock-skew margin from the job's clock. A writer whose clock lags
  by more than an hour stamps `committed_at_ms` and `expires_at_ms` values
  the margin does not cover, so its rows can be treated as expired, and its
  manifests as aged, up to that much earlier than it intended.
- **The 30-day age bound is exercised only by kernel tests until a root has
  30 days of history.** Worker tests cannot inject the kernel clock, so they
  cover the expiry purge only; the age bound is covered by the kernel's
  tests and the durable model. On a root younger than 30 days the
  `age_anchor` chain ends above the floor, the age bound is zero and the
  horizon purges no tombstones (`purged_tombstones=0`); only expired rows
  are purged.
- **A catalog root carrying generic trim-binding metadata fails the drain
  and trim phases.** The fixed-consumer drain and trim refuse such a root
  with a non-deferrable invariant violation whose message is `fixed
  projection consumer cannot use a source root with generic binding
  metadata`; the `phase failed` lines for `drain` and `trim` carry it and
  the job exits non-zero on every run until the key is removed from the
  catalog root. The worker never writes that key; it appears only if a
  generic `ProjectionOutboxWorker::trim_acked` or rebind was run against the
  catalog root, which the operator endpoint refuses for `catalog`.
  Acknowledgements an in-flight trim had already retired are redelivered by
  the next fixed drain once the root is unbound.
- The job maintains exactly the `catalog` and `projection-outbox-acks` domains.
  Other control domains need their own entry in `CONTROL_DOMAINS`.

## Related

- `docs/runbooks/state-store-projection-lag.md`
- `docs/runbooks/state-store-replay-budget.md`
- `docs/runbooks/control-plane-repair-and-dark-launch.md`
- `docs/runbooks/gc-failure.md`
- `docs/adr/adr-043-s3-state-token-authority.md`
