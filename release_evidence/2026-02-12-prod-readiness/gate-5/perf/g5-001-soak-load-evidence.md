# G5-001 Soak + 10x Load Evidence

Generated UTC: 2026-02-15T19:29:45Z
Status: BLOCKED-EXTERNAL
Owner: Perf Engineering

## Closure Requirement

Run a 24h soak and 10x load profile in staging with SLO/error-budget validation and archived artifacts.

## Fresh Evidence Captured (Local)

1. Runbook presence check passed.
2. Benchmark harness compile regression is fixed:
   - `cargo bench -p arco-catalog --bench catalog_lookup --no-run`
   - Result: `Finished bench profile` + benchmark executable emitted.
3. Load test tools are still unavailable in this workspace image (`k6`, `vegeta`, `locust` missing).

## Why This Signal Is Still Open

1. 24h soak and 10x load require authenticated staging traffic generation and live observability capture.
2. Required load generator binaries are not installed in this execution environment.
3. SLO/error-budget evaluation must use staging telemetry exports, which are external to this workspace.

## Evidence Artifacts

- Command status matrix:
  - `release_evidence/2026-02-12-prod-readiness/gate-5/perf/g5-001-command-status.tsv`
- Key logs:
  - `release_evidence/2026-02-12-prod-readiness/gate-5/perf/command-logs/20260215T192346Z_bench_catalog_lookup_no_run_post_fix.log`
  - `release_evidence/2026-02-12-prod-readiness/gate-5/perf/command-logs/20260215T192417Z_load_tooling_preflight_post_fix.log`

## External Handoff Steps

| Step | Owner | Command | Expected artifact | Destination |
|---|---|---|---|---|
| Install and verify load tooling | Perf Engineering | `command -v k6 && command -v vegeta && command -v locust` | tool presence log | `gate-5/perf/command-logs/load_tooling_presence.log` |
| Execute 10x load profile | Perf Engineering | `TARGET_URL=<staging-url> k6 run <k6_10x_profile.js> --env TARGET_URL=$TARGET_URL` | latency/error summary + raw output | `gate-5/perf/load_10x_results.log`, `gate-5/perf/load_10x_summary.md` |
| Execute 24h soak | Perf Engineering | `TARGET_URL=<staging-url> k6 run <k6_soak_profile.js> --duration 24h --env TARGET_URL=$TARGET_URL` | 24h soak output + failure timeline | `gate-5/perf/soak_24h_results.log`, `gate-5/perf/soak_24h_summary.md` |
| Export SLO/error-budget telemetry | Perf Engineering + Observability | `curl -fsS \"${PROM_URL}/api/v1/query_range\" --data-urlencode 'query=histogram_quantile(0.95,sum(rate(http_request_duration_seconds_bucket[5m])) by (le))' --data-urlencode 'start=<RFC3339>' --data-urlencode 'end=<RFC3339>' --data-urlencode 'step=60'` | p95/p99/error-budget exports | `gate-5/perf/load_metrics_export.json`, `gate-5/perf/soak_metrics_export.json` |
