# Phase 3C Promotion Gate — First Evidence-Packet Dry Run

**Date:** 2026-07-30
**Substrate:** `ControlMvpStateStore` with checkpoint-anchored bounded replay
(format version 2) and writer-epoch fencing, `MemoryBackend`, automatic
checkpoint interval 32.
**Harness:** `cargo bench -p arco-catalog --bench control_mvp_gate`
**Gate:** `arco_catalog::state_store::promotion_gate` with the roadmap's
numeric budgets encoded as typed thresholds (warm write p99 ≤ 250ms, warm
point-read p99 ≤ 50ms, cold writer startup ≤ 2s, manifest-reachable replay
≤ 64MiB).

## Decision: `reject_advancement` — and that is the correct, honest outcome

This dry run exists to prove the gate can now actually run with real
measurements and produce a durable evidence packet, not to promote the
prototype. MemoryBackend measurements are **not** production-provider
evidence. The gate rejects because the criteria that require real-provider,
projection, and operational evidence are truthfully unsatisfied (see
"What a real run still requires").

## Measured results

Workload: 256 setup commits across 64 keys, then 200 timed samples per
quantity (20 cold starts). History length at measurement time: 456 commits.

| Measurement | Value | Budget | Within budget |
|---|---|---|---|
| Warm write p99 (narrow metadata mutation) | 412 µs | 250,000 µs | yes |
| Warm point-read p99 | 170 µs | 50,000 µs | yes |
| Bounded prefix-scan p99 | 171 µs | none | n/a |
| Cold writer startup to write-ready (p99) | 172 µs | 2,000,000 µs | yes |
| Manifest-reachable replay bytes (cold read) | 19,819 B over 11 GETs | 67,108,864 B | yes |
| Projection watermark lag | unavailable | none | no — no projection pipeline exists |
| Compaction backlog before replay-budget breach | 37,262 txs (derived from mean fetched-object size) | none | n/a |
| StateToken read-after-write retention | 456 tokens (all manifests retained; no GC) | none | n/a |

**The bounded-replay fix (#334) is visible directly in the numbers:** a cold
read at 456-commit history costs 11 GETs (~19.4 KB) — pointer + manifest +
anchor snapshot + the tx suffix since the last anchor — where the pre-fix
implementation performed one GET per historical transaction (456 GETs) and
grew without bound. Replay cost is now independent of total history length
(`replay_after_anchor_is_bounded_independent_of_history_length` asserts this
in CI with a counting backend).

## Criteria assessment (what was claimed, what was not)

Satisfied (backed by CI-executed suites in this tree):

- `correctness_and_failure_state_tests` — 33 control-MVP tests including CAS
  loss, orphan invisibility, three-layer corruption fail-closed, snapshot
  corruption fail-closed, boundary-commit crash recovery, epoch fencing.
- `state_token_read_after_write` — token-pinned reads exercised in tests and
  in this harness.
- `model_replay_equivalence` — deterministic model suite.
- `object_store_mvp_replay_equivalence` — manifest replay equals folded KV,
  now through checkpoint anchors.

Deliberately NOT claimed:

- `provider_cas_and_retry_behavior` — only MemoryBackend evidence exists;
  GCS/S3 conformance remains `#[ignore]`d and skip-green in CI (#366).
- `projection_equality_watermark` — no projection pipeline exists for this
  store.
- `enforcement_and_vending_freshness` — no enforcement-seam caller consumes
  the store.
- `operational_complexity_acceptable` — no alerts, dashboards, or runbooks
  exist for any control-store failure state.

## What a real promotion run still requires

1. **Real-provider evidence (#366):** GCS and S3 CAS/retry conformance runs
   that fail loudly when unconfigured, plus the sole-writer IAM smoke test
   actually executing (today `iam_smoke` is triple-gated and skips green).
   The `state-store/` object prefix also needs an IAM condition — none of the
   deployed `iam_conditions.tf` expressions match it.
2. **Provider-latency measurement:** this harness re-run against real GCS/S3
   with production-shaped payloads; the µs-scale numbers above will become
   ms-scale and must still clear the budgets.
3. **Projection watermark evidence:** a projection pipeline (or the Phase 5
   ack-domain wiring) that makes `projection_watermark_lag` measurable.
4. **Operational evidence:** alerts, dashboards, and runbooks for writer
   lease loss, token expiry, projection lag, corrupt artifacts, and CAS
   failure states.
5. **Comparison baselines:** evaluate against the current-path
   read-amplification and DDL write-amplification baselines recorded in
   `docs/reports/2026-06-27-batch7-performance-architecture-docs.md`
   (#279/#280 rows).

Until every criterion is satisfied with that evidence, the fallback holds:
current synchronous-compactor authority remains; derived indexes and
projection acceleration only; no cutover of catalog DDL, grants, credential
vending, or broad governance.

## Serialized gate report (evidence packet)

```json
{
  "decision": "reject_advancement",
  "criteria": [
    {
      "criterion": "correctness_and_failure_state_tests",
      "satisfied": true
    },
    {
      "criterion": "provider_cas_and_retry_behavior",
      "satisfied": false
    },
    {
      "criterion": "state_token_read_after_write",
      "satisfied": true
    },
    {
      "criterion": "model_replay_equivalence",
      "satisfied": true
    },
    {
      "criterion": "object_store_mvp_replay_equivalence",
      "satisfied": true
    },
    {
      "criterion": "projection_equality_watermark",
      "satisfied": false
    },
    {
      "criterion": "enforcement_and_vending_freshness",
      "satisfied": false
    },
    {
      "criterion": "operational_complexity_acceptable",
      "satisfied": false
    }
  ],
  "measurements": [
    {
      "measurement": {
        "kind": "warm_write_p99_narrow_metadata_mutation",
        "source": "opt_in_benchmark",
        "value": {
          "unit": "duration_micros",
          "amount": 412
        }
      },
      "budget": {
        "unit": "duration_micros",
        "amount": 250000
      },
      "satisfied": true
    },
    {
      "measurement": {
        "kind": "warm_point_read_p99",
        "source": "opt_in_benchmark",
        "value": {
          "unit": "duration_micros",
          "amount": 170
        }
      },
      "budget": {
        "unit": "duration_micros",
        "amount": 50000
      },
      "satisfied": true
    },
    {
      "measurement": {
        "kind": "bounded_prefix_scan_p99",
        "source": "opt_in_benchmark",
        "value": {
          "unit": "duration_micros",
          "amount": 171
        }
      },
      "budget": null,
      "satisfied": true
    },
    {
      "measurement": {
        "kind": "cold_writer_startup_to_write_ready",
        "source": "opt_in_benchmark",
        "value": {
          "unit": "duration_micros",
          "amount": 172
        }
      },
      "budget": {
        "unit": "duration_micros",
        "amount": 2000000
      },
      "satisfied": true
    },
    {
      "measurement": {
        "kind": "manifest_reachable_replay_bytes",
        "source": "opt_in_benchmark",
        "value": {
          "unit": "bytes",
          "amount": 19819
        }
      },
      "budget": {
        "unit": "bytes",
        "amount": 67108864
      },
      "satisfied": true
    },
    {
      "measurement": {
        "kind": "projection_watermark_lag",
        "source": "unavailable",
        "value": null
      },
      "budget": null,
      "satisfied": false
    },
    {
      "measurement": {
        "kind": "compaction_backlog_before_replay_budget_breach",
        "source": "opt_in_benchmark",
        "value": {
          "unit": "count",
          "amount": 37262
        }
      },
      "budget": null,
      "satisfied": true
    },
    {
      "measurement": {
        "kind": "state_token_read_after_write_retention",
        "source": "opt_in_benchmark",
        "value": {
          "unit": "count",
          "amount": 456
        }
      },
      "budget": null,
      "satisfied": true
    }
  ],
  "fallback_recommendation": {
    "keep_current_synchronous_compactor_authority": true,
    "continue_derived_indexes_and_projection_acceleration_only": true,
    "do_not_cut_over": [
      "catalog DDL",
      "grants",
      "credential vending",
      "broad governance"
    ]
  }
}
```
