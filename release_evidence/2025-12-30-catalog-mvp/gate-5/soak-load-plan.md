# Load and Soak Test Plan (Draft)

Date: 2025-12-31
Status: DRAFT (not executed; requires deployed env)

## Goals
- Validate stability under sustained load (soak)
- Identify throughput and latency limits (load)
- Confirm compactor keeps up (lag and error budgets)

## Prerequisites
- Deployed dev/staging environment
- Monitoring stack running (metrics, logs, traces)
- Test tenant/workspace provisioned
- Seed data and API auth configured

## Tooling Options
- k6 (preferred for HTTP + JSON)
- vegeta (simple HTTP throughput)
- Locust (if Python-based is preferred)

## Test Scenarios

### 1) Baseline Load (15 minutes)
- Constant RPS: 5
- Targets: Read-heavy API endpoints
- Success: p95 < 500ms, error rate < 1%

### 2) Ramp Load (30 minutes)
- Ramp from 5 RPS to 50 RPS
- Mix: 70% reads, 30% writes
- Success: p95 < 1s, error rate < 1%

### 3) Spike Test (10 minutes)
- Spike from 10 RPS to 100 RPS for 2 minutes
- Success: no crash; recovery within 5 minutes

### 4) Soak Test (24 hours)
- Constant 5-10 RPS
- Success: no memory leak, steady latency, compactor lag < 10 minutes

## Metrics to Capture
- API latency (p50, p95, p99)
- Error rate (5xx, timeouts)
- CPU/memory for API and compactor
- Queue/backlog depth (if applicable)
- Compaction lag (watermark vs latest event)

## Pass/Fail Criteria
- No sustained error rate > 1%
- No OOM or crash loops
- Compactor watermark advances continuously
- Resource usage stable over 24h soak

## Evidence Artifacts
- Load test command output (log)
- Metrics screenshots/exports
- Compactor lag chart
- Incident/alert log (if any)

## Next Steps (when env exists)
1. Provision test tenant/workspace
2. Run baseline and ramp tests
3. Run spike test
4. Run 24h soak test
5. Store artifacts in `release_evidence/2025-12-30-catalog-mvp/gate-5/`
