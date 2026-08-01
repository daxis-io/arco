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

- `ArcoProjectionWatermarkLagHigh` — lag in logical sequences
  (`committed_sequence - latest_projected_sequence`, the `pending_sequences`
  computation in `projection_watermark_lag_for`).
- `ArcoProjectionWatermarkStale` — watermark age beyond the 60 s budget.
- `ArcoProjectionPublishAbsent` — no projection publish completed in an hour.

## Diagnosis

1. Determine the committed head: read
   `state-store/control-mvp/{domain}/current.pointer.json` and note
   `logical_sequence`.
2. Determine the projected watermark: query the projection consumer's recorded
   latest projected sequence (once a production publisher exists this is the
   consumer's ack watermark; today the only implementations are the hermetic
   ack surfaces above).
3. Classify:
   - `ProjectionUnavailable` (no watermark at all): the projection consumer
     never ran or lost its state — treat as publish absence;
   - `StaleProjection` with a growing gap: the consumer is running behind or
     wedged;
   - stale age but zero sequence lag: a low-volume domain with a stalled
     clock/heartbeat rather than real backlog.
4. Check the projection publisher process (when wired: the control-store
   projection compactor). Control writes continuing while watermarks lag is
   the designed degradation mode — availability of writes is never coupled to
   projection health.
5. Rule out the corrupt-artifact case: if the consumer fails while replaying
   outbox records (`current_projection_outbox()` errors), follow
   `docs/runbooks/state-store-corrupt-artifact.md`.

## Remediation

- Restart or redeploy the projection consumer; it must resume from its last
  acked record id (acks are idempotent: re-acking the same
  `(consumer_id, record_id)` pair is a no-op returning the existing receipt).
- Drain backlog: the consumer reads pending records through
  `current_projection_outbox()` / `projection_outbox_at(token)` and acks each
  processed record; no manual object surgery is involved.
- If the watermark update itself keeps failing, retry the watermark publish;
  watermark publication is CAS-guarded like every other control write.
- While lag persists, verify staleness is surfaced explicitly wherever the
  projection is consumed (system tables must not present stale rows as fresh).
- Do not point readers at raw projection files to "work around" lag —
  enforcement and reads must come from authority or explicitly-stale
  projections only.

## Current Wiring Status

Honest status as of 2026-07-30 (program audit): there is no production
projection publisher or consumer. The outbox-ack writer, freshness, and
watermark-lag types are implemented and tested but marked `#[allow(dead_code)]`
with no production callers; the storage-governance projection publisher is an
open issue (#362). The `arco_state_store_projection_*` metrics are reserved in
`crates/arco-catalog/src/metrics.rs` with no emitter, so none of the detection
alerts can fire yet. Diagnosis step 2 is aspirational until that wiring lands.
