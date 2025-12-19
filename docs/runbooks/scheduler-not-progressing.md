# Runbook: Scheduler Not Making Progress

## Alert

**Alert Name:** `ArcoFlowSchedulerStalled`
**Severity:** Critical
**Condition:** `arco_flow_scheduler_tick_duration_seconds_count` has not increased for > 5 minutes

## Symptoms

- Tasks remain in `Ready` state indefinitely
- `arco_flow_active_runs` gauge is non-zero but stable
- No new `arco_flow_tasks_total` transitions observed
- Worker pools report idle capacity

## Impact

- Pipeline SLAs at risk
- Data freshness degraded
- Downstream consumers blocked on stale data

## Investigation Steps

### 1. Check Scheduler Leader Status

```bash
# Query the scheduler leader lease
kubectl exec -it postgres-0 -- psql -U arco -d arco -c \
  "SELECT * FROM scheduler_leader WHERE lock_key = 'scheduler';"
```

**Expected:** One row with `holder_id` matching an active scheduler instance and `expires_at` in the future.

**If lease expired:** The scheduler failed to renew its lease. Check scheduler pod logs.

### 2. Check Scheduler Pod Health

```bash
# List scheduler pods
kubectl get pods -l app=arco-scheduler

# Check logs for errors
kubectl logs -l app=arco-scheduler --tail=200 | grep -i "error\|panic\|warn"
```

**Common issues:**
- OOM kills: Check memory usage with `kubectl top pods`
- Crash loops: Check restart count in `kubectl describe pod`
- Network partition: Check connectivity to Postgres and Cloud Tasks

### 3. Check Task Queue Backpressure

```bash
# Query task queue depth
curl -s http://scheduler:9090/metrics | grep arco_flow_dispatch_queue_depth
```

**If queue depth is high:**
- Cloud Tasks queue may be rate-limited
- Worker pool may be saturated
- Check Cloud Tasks console for queue health

### 4. Check Database Connection Pool

```bash
# Check active connections
kubectl exec -it postgres-0 -- psql -U arco -d arco -c \
  "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'arco%';"
```

**If connections exhausted:** Increase pool size or investigate connection leaks.

### 5. Check for Deadlocks

```bash
# Check for blocking queries
kubectl exec -it postgres-0 -- psql -U arco -d arco -c \
  "SELECT blocked_locks.pid AS blocked_pid,
          blocked_activity.usename AS blocked_user,
          blocking_locks.pid AS blocking_pid,
          blocking_activity.usename AS blocking_user,
          blocked_activity.query AS blocked_statement
   FROM pg_catalog.pg_locks blocked_locks
   JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
   JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
   JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
   WHERE NOT blocked_locks.granted AND blocked_locks.pid != blocking_locks.pid;"
```

## Resolution Steps

### Force Leader Re-election

If the current leader is unhealthy but still holding the lease:

```bash
# Delete the scheduler pod to trigger failover
kubectl delete pod arco-scheduler-0

# Verify new leader acquired lease
kubectl exec -it postgres-0 -- psql -U arco -d arco -c \
  "SELECT * FROM scheduler_leader WHERE lock_key = 'scheduler';"
```

### Restart Scheduler Deployment

```bash
kubectl rollout restart deployment/arco-scheduler
kubectl rollout status deployment/arco-scheduler
```

### Scale Scheduler Replicas

If a single scheduler is overwhelmed:

```bash
# Note: Only one scheduler is active at a time (leader election)
# Scaling provides faster failover but not more throughput
kubectl scale deployment/arco-scheduler --replicas=3
```

### Clear Stuck Tasks

If tasks are stuck due to corrupted state (last resort):

```sql
-- Identify stuck tasks
SELECT task_id, state, last_transition_at
FROM task_executions
WHERE state IN ('Ready', 'Dispatched')
  AND last_transition_at < NOW() - INTERVAL '30 minutes';

-- Reset to Pending (allows re-scheduling)
UPDATE task_executions
SET state = 'Pending', last_transition_reason = 'manual_reset'
WHERE state IN ('Ready', 'Dispatched')
  AND last_transition_at < NOW() - INTERVAL '30 minutes';
```

## Prevention

1. **Monitoring:** Set up alerts on scheduler tick duration
2. **Capacity Planning:** Monitor queue depth and worker utilization
3. **Health Checks:** Ensure liveness/readiness probes are configured
4. **Connection Pooling:** Use PgBouncer for connection management

## Related Metrics

| Metric | Threshold | Alert |
|--------|-----------|-------|
| `arco_flow_scheduler_tick_duration_seconds_count` rate | < 0.1/s for 5m | Stalled |
| `arco_flow_scheduler_tick_duration_seconds_p99` | > 10s | Slow |
| `arco_flow_active_runs` | > 100 | Backlog |

## Escalation

If unable to resolve within 30 minutes:
1. Page on-call engineer
2. Open incident channel
3. Consider pausing upstream data ingestion
