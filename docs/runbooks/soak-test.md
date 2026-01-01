# Runbook: Soak Test

## Status
Deferred (requires staging environment)

## Overview
Validate stability under sustained load (memory growth, error rates, compaction health).

## Preconditions
- Staging environment with production-like data volume
- Traffic generator capable of a 24h run
- Monitoring dashboards and alerts enabled

## Procedure
1. Start steady-state traffic for catalog reads, writes, and query endpoints.
2. Maintain load for 24 hours.
3. Monitor error rate, latency, CPU, memory, and storage growth.
4. Capture any alerts and investigate regressions.

## Evidence to capture
- Start/end metrics export
- Error logs and alert timeline
- Storage growth summary

## Rollback
If errors exceed thresholds, stop traffic and restore the prior release.
