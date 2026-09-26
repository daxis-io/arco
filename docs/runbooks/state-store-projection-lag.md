# Runbook: Projection Lag / Stale Watermark

Failure states (Tier-1 control-store strategy, 2026-06-25, Failure States
table): "compactor is down — control writes continue; projection watermarks
lag; system tables expose stale metadata", "projection watermark update fails —
retry watermark publish through chosen root", and "projection compactor writes
partial files — no new projection visible; retry projection publication."

Budget: projection watermark lag <= 60 s target for low-volume Tier-1 domains;
beyond that, staleness must be explicit, never silent.

## Symptoms

- Committed control-store writes are not visible through projections or system
  tables while direct authority reads are current.
- Freshness classification returns
  `ProjectionOutboxAckFreshness::StaleProjection { committed_sequence, latest_projected_sequence }`
  or `ProjectionUnavailable`
  (`crates/arco-catalog/src/state_store/projection_outbox_acks.rs`).
- Comparison reads report `stale_projection` diagnostics
  (`CatalogInventoryComparisonStatus::StaleProjection`,
  `crates/arco-catalog/src/state_store/comparison_reads.rs`) — these are
  diagnostic-only and never fail the primary read.
- The projection outbox backlog grows: records staged by
  `ControlMvpTxn::stage_projection_outbox` keep accumulating in the visible
  manifest chain without acks.

## Detection

Alerts (`infra/monitoring/alerts.yaml`, group `arco.state_store`):

- `ArcoProjectionWatermarkLagHigh` — backlog depth in logical sequences
  (`committed_sequence - latest_projected_sequence`, the `pending_sequences`
  computation in `projection_watermark_lag_for`) above 1000 for 10 minutes.
  The threshold is a backlog budget, not a contract budget: a non-zero gap is
  the normal steady state between a commit and the next publish, so only a
  sustained backlog means the consumer is losing ground.
- `ArcoProjectionWatermarkStale` — watermark age beyond the 60 s budget. This
  is the contract-derived rule; it catches a small gap that is not advancing,
  which the backlog-depth rule above deliberately ignores.
- `ArcoProjectionPublishAbsent` — no projection publish completed in an hour,
  evaluated against the watermark-age gauge as an anchor series so that it
  still fires when the publish counter goes stale (publisher dead) or never
  existed (domain never published). An alert instance with an empty `domain`
  label means the projection metrics are not being exported at all.

## Diagnosis

1. Determine the committed head: read
   `tenant={tenant}/workspace={workspace}/control/v1/domains/{domain}/head/current.json`
   and note `logical_sequence`.
2. Determine the projected watermark: query
   `system.catalog.projection_status` (built from
   `ProjectionOutboxAckWriter::projection_status` and
   `ProjectionOutboxWorker::backlog`, `crates/arco-api/src/system_tables.rs`;
   `CatalogProjectionMaterializer::status` reads the same durable status and
   is what the worker job logs), or read the consumer's ack watermark in the
   separate acknowledgement root
   (`control/v1/domains/projection-outbox-acks/head/current.json` under the
   same workspace prefix).
3. Classify:
   - `ProjectionUnavailable` (no watermark at all): the projection consumer
     never ran or lost its state — treat as publish absence;
   - `StaleProjection` with a growing gap: the consumer is running behind or
     wedged;
   - stale age but zero sequence lag: a low-volume domain with a stalled
     clock/heartbeat rather than real backlog.
4. Check the projection drain. As of 2026-09-23 the scheduled
   `arco-control-store-worker` job (`docs/runbooks/control-store-worker.md`)
   runs `CatalogProjectionMaterializer::drain_once`, and an operator can drain
   on demand through `POST /internal/control-store/projection-outbox` (mounted
   only with `ARCO_CONTROL_STORE_OPERATOR_ENDPOINTS=true`, never on a public
   posture, and requiring the verified principal to carry
   `ARCO_CONTROL_STORE_OPERATOR_GROUP`). The API also attempts a fail-open
   process-local wake after each committed intent. Control writes continuing
   while watermarks lag is the designed degradation mode — availability of
   writes is never coupled to projection health.
5. Rule out the corrupt-artifact case: if the consumer fails while replaying
   outbox records (`current_projection_outbox()` errors), follow
   `docs/runbooks/state-store-corrupt-artifact.md`.

## Remediation

- Re-run the drain (`gcloud run jobs execute "arco-control-store-worker-${ENV}"`,
  or the operator endpoint above); it resumes from its last acked record id
  (acks are idempotent: re-acking the same `(consumer_id, record_id)` pair is
  a no-op returning the existing receipt), and drains are coalesced with ack
  retries, so a repeated invocation is safe.
- Drain backlog: `drain_once` reads pending records through
  `current_projection_outbox()` / `projection_outbox_at(token)`, materializes
  each Parquet artifact, and acks each processed record; no manual object
  surgery is involved. A malformed intent receives a sticky terminal
  quarantine status and stays visible as unresolved backlog while later valid
  intents continue — it needs an operator decision, not another retry.
- If the watermark update itself keeps failing, retry the watermark publish;
  watermark publication is CAS-guarded like every other control write.
- While lag persists, verify staleness is surfaced explicitly wherever the
  projection is consumed (system tables must not present stale rows as fresh).
- Do not point readers at raw projection files to "work around" lag —
  enforcement and reads must come from authority or explicitly-stale
  projections only.

## Current Wiring Status

Status as of 2026-09-23: the projection consumer is
`CatalogProjectionMaterializer` (`crates/arco-catalog/src/catalog_authority.rs`),
which materializes real Parquet artifacts, acks into the separate
`projection-outbox-acks` root, and exposes durable status through
`system.catalog.projection_status`. It runs from the scheduled
`arco-control-store-worker` job and the operator drain endpoint; provider
queue delivery and always-on wake do not exist, so the job's cron cadence
cannot meet the 10 s p99 lag objective in ADR-043. The
`arco_state_store_projection_watermark_lag_sequences`,
`arco_state_store_projection_watermark_age_seconds`, and
`arco_state_store_projection_publish_total` emitters exist as of 2026-09-23,
so the detection alerts can fire once a root is bound. The control authority
itself is route-wired for one exact root behind `ARCO_CATALOG_CONTROL_V1_*`,
legacy by default, not provider-qualified, and not authoritative on any
deployed root; the storage-governance projection publisher remains an open
issue (#362).
