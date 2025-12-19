# Runbook: High Task Failure Rate

## Alert

**Alert Name:** `ArcoFlowHighTaskFailureRate`
**Severity:** Warning / Critical
**Condition:**
- Warning: `rate(arco_flow_tasks_total{to_state="failed"}[5m]) / rate(arco_flow_tasks_total{to_state=~"succeeded|failed"}[5m]) > 0.1` (>10%)
- Critical: `rate(arco_flow_tasks_total{to_state="failed"}[5m]) / rate(arco_flow_tasks_total{to_state=~"succeeded|failed"}[5m]) > 0.5` (>50%)

## Symptoms

- Increased `arco_flow_tasks_total{to_state="failed"}` counter
- Increased `arco_flow_retries_total` counter
- Runs failing to complete
- Data freshness degraded

## Impact

- Pipeline SLAs at risk
- Data consumers receiving stale data
- Resource waste due to retries
- Potential cascading failures in dependent pipelines

## Common Causes

### 1. Upstream Data Issues

- Source data schema changed
- Source data quality degraded
- Source system returning errors

### 2. Resource Exhaustion

- OOM kills in workers
- CPU throttling
- Disk space exhaustion

### 3. Infrastructure Issues

- Database connection failures
- Cloud storage errors
- Network timeouts

### 4. Code Bugs

- Recent deployment introduced regression
- Edge case in data handling
- Dependency version conflict

### 5. Configuration Drift

- Credentials expired
- Environment variable missing
- Service account permissions changed

## Investigation Steps

### 1. Identify Failure Pattern

```sql
-- Get failure distribution by asset
SELECT
    asset_key,
    COUNT(*) AS failure_count,
    COUNT(DISTINCT run_id) AS affected_runs
FROM task_executions
WHERE state = 'Failed'
  AND last_transition_at > NOW() - INTERVAL '1 hour'
GROUP BY asset_key
ORDER BY failure_count DESC
LIMIT 20;
```

```sql
-- Get recent failure reasons
SELECT
    task_id,
    asset_key,
    last_transition_reason,
    error_message,
    last_transition_at
FROM task_executions
WHERE state = 'Failed'
  AND last_transition_at > NOW() - INTERVAL '1 hour'
ORDER BY last_transition_at DESC
LIMIT 50;
```

### 2. Check for Resource Issues

```bash
# Check worker memory usage
kubectl top pods -l app=arco-worker

# Check for OOM kills
kubectl get events --field-selector=reason=OOMKilled

# Check node resource pressure
kubectl describe nodes | grep -A5 "Conditions:"
```

### 3. Check Worker Logs

```bash
# Get error logs from workers
kubectl logs -l app=arco-worker --since=30m | grep -i "error\|exception\|panic" | head -100

# Filter by specific task
kubectl logs -l app=arco-worker --since=30m | grep <task_id>
```

### 4. Check Upstream Data Source

```bash
# If failures correlate with a specific asset, check its source
# Example: Check BigQuery source table
bq show --schema project:dataset.source_table

# Check recent data in source
bq query --max_rows=10 "SELECT * FROM project:dataset.source_table ORDER BY _PARTITIONTIME DESC LIMIT 10"
```

### 5. Check Recent Deployments

```bash
# List recent deployments
kubectl rollout history deployment/arco-worker

# Compare with failure spike timing
kubectl get events --sort-by='.lastTimestamp' | grep -i "deployment\|rollout"
```

### 6. Check Credentials and Permissions

```bash
# Verify service account
kubectl get serviceaccount arco-worker -o yaml

# Test BigQuery access
kubectl exec -it arco-worker-0 -- bq ls

# Test GCS access
kubectl exec -it arco-worker-0 -- gsutil ls gs://arco-data/
```

## Resolution Steps

### Immediate Mitigation

#### 1. Pause Affected Pipelines

If failures are cascading, pause the affected pipelines:

```sql
-- Cancel running runs for a specific tenant
UPDATE runs
SET state = 'Cancelled', cancel_reason = 'high_failure_rate_investigation'
WHERE tenant_id = 'affected_tenant'
  AND state IN ('Running', 'Pending');
```

#### 2. Rollback Recent Deployment

If failure correlates with a deployment:

```bash
kubectl rollout undo deployment/arco-worker
kubectl rollout status deployment/arco-worker
```

#### 3. Increase Resource Limits

If OOM is the cause:

```bash
kubectl patch deployment arco-worker -p '{"spec":{"template":{"spec":{"containers":[{"name":"worker","resources":{"limits":{"memory":"4Gi"}}}]}}}}'
```

### Root Cause Fixes

#### Schema Migration Issues

1. Update asset definition to handle new schema
2. Add backward-compatible parsing
3. Coordinate with upstream on schema versioning

#### Credential Expiry

1. Rotate credentials
2. Update Kubernetes secrets
3. Restart affected pods

```bash
kubectl create secret generic arco-credentials --from-file=key.json --dry-run=client -o yaml | kubectl apply -f -
kubectl rollout restart deployment/arco-worker
```

#### Resource Limits

1. Profile memory usage of failing tasks
2. Adjust ResourceRequirements in task specs
3. Consider splitting large tasks

### Retry Failed Tasks

After fixing the root cause:

```sql
-- Reset failed tasks for retry
UPDATE task_executions
SET
    state = 'Ready',
    attempt = attempt + 1,
    last_transition_reason = 'manual_retry_after_fix',
    last_transition_at = NOW()
WHERE state = 'Failed'
  AND run_id IN (SELECT run_id FROM runs WHERE state = 'Running')
  AND attempt < max_attempts;
```

## Prevention

### 1. Circuit Breakers

Implement circuit breakers that pause scheduling when failure rate exceeds threshold:

```yaml
# Scheduler config
circuit_breaker:
  enabled: true
  failure_threshold: 0.3  # 30% failure rate
  reset_timeout: 5m
```

### 2. Canary Deployments

Deploy to a subset of workers first:

```bash
kubectl patch deployment arco-worker -p '{"spec":{"strategy":{"rollingUpdate":{"maxUnavailable":"25%","maxSurge":"25%"}}}}'
```

### 3. Pre-flight Checks

Add validation before task execution:
- Schema validation
- Credential validation
- Resource availability check

### 4. Improved Observability

- Add structured error classification
- Implement distributed tracing
- Create dashboards per asset type

## Related Metrics

| Metric | Threshold | Alert |
|--------|-----------|-------|
| `arco_flow_tasks_total{to_state="failed"}` rate | > 10/min | High failures |
| `arco_flow_retries_total` rate | > 50/min | Retry storm |
| `arco_flow_task_duration_seconds{state="failed"}` p50 | < 5s | Fast failures (likely config) |
| `arco_flow_task_duration_seconds{state="failed"}` p50 | > 300s | Timeout failures |

## Failure Classification

| Error Type | Typical Cause | Action |
|------------|---------------|--------|
| Fast failure (<5s) | Configuration, auth | Check credentials, config |
| Timeout failure | Resource exhaustion, slow source | Increase limits, check source |
| Intermittent | Network, rate limiting | Retry with backoff |
| 100% failure rate | Breaking change | Rollback, investigate |

## Escalation

1. **>10% failure rate for 15 min:** Page on-call
2. **>50% failure rate:** Incident channel, consider pausing pipelines
3. **Data freshness SLA breach:** Notify downstream consumers

## Post-Incident

1. Update runbook with new failure mode
2. Add detection for this failure type
3. Consider adding automated mitigation
