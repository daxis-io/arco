# Runbook: Task Stuck in Dispatched State

## Alert

**Alert Name:** `ArcoFlowTaskDispatchedTooLong`
**Severity:** Warning
**Condition:** Tasks in `Dispatched` state for > 5 minutes without heartbeat

## Symptoms

- Tasks remain in `Dispatched` state beyond the dispatch-ack timeout (default: 30s)
- Scheduler reports zombie tasks but cannot recover them
- Worker logs show no record of receiving the task

## Impact

- Individual tasks delayed
- Downstream dependencies blocked
- Pipeline latency increased

## Root Causes

### 1. Worker Failed to Receive Task (Most Common)

The task was enqueued to Cloud Tasks but the worker never received it:
- Cloud Tasks delivery failure
- Worker pod crashed between dispatch and ack
- Network partition between Cloud Tasks and worker

### 2. Worker Processing Silently

The worker is processing but not sending heartbeats:
- Heartbeat mechanism failed
- Worker stuck in blocking operation
- Worker overloaded (heartbeat thread starved)

### 3. Duplicate Dispatch Conflict

Multiple schedulers attempted to dispatch the same task:
- Leader election race condition
- Idempotency key collision

## Investigation Steps

### 1. Identify Stuck Tasks

```sql
SELECT
    t.task_id,
    t.run_id,
    t.state,
    t.dispatched_at,
    t.last_heartbeat,
    t.attempt,
    EXTRACT(EPOCH FROM (NOW() - t.dispatched_at)) AS age_seconds
FROM task_executions t
WHERE t.state = 'Dispatched'
  AND t.dispatched_at < NOW() - INTERVAL '5 minutes'
ORDER BY t.dispatched_at ASC;
```

### 2. Check Cloud Tasks Queue

```bash
# List tasks in queue (requires gcloud CLI)
gcloud tasks list --queue=arco-tasks --location=us-central1 --limit=50

# Get specific task details
gcloud tasks describe <task-name> --queue=arco-tasks --location=us-central1
```

**Look for:**
- Task status (PENDING, RUNNING, FAILED)
- Last attempt time
- Dispatch count
- Response code from last attempt

### 3. Check Worker Logs

```bash
# Find which worker should have received the task
kubectl logs -l app=arco-worker --since=30m | grep <task_id>

# Check for errors during task receipt
kubectl logs -l app=arco-worker --since=30m | grep -i "error\|panic" | head -50
```

### 4. Check Scheduler Dispatch Logs

```bash
# Verify scheduler dispatched the task
kubectl logs -l app=arco-scheduler --since=30m | grep <task_id>
```

### 5. Check Network Connectivity

```bash
# From worker pod, verify Cloud Tasks connectivity
kubectl exec -it arco-worker-0 -- curl -v https://cloudtasks.googleapis.com/

# Check DNS resolution
kubectl exec -it arco-worker-0 -- nslookup cloudtasks.googleapis.com
```

## Resolution Steps

### Force Task Recovery (Scheduler Handles This)

The scheduler should automatically detect zombie tasks via `dispatch_ack_timeout`. Verify this is working:

```bash
# Check scheduler zombie detection
kubectl logs -l app=arco-scheduler --since=10m | grep "zombie"
```

If the scheduler is not recovering zombie tasks:

1. **Check timeout configuration:**
```bash
kubectl get configmap arco-scheduler-config -o yaml | grep -i timeout
```

2. **Force a scheduler tick:**
```bash
# Restart scheduler to force immediate zombie check
kubectl delete pod -l app=arco-scheduler
```

### Manual Task Reset (Emergency Only)

If automatic recovery is not working:

```sql
-- Reset task to Ready state for re-dispatch
UPDATE task_executions
SET
    state = 'Ready',
    dispatched_at = NULL,
    last_heartbeat = NULL,
    attempt = attempt + 1,
    last_transition_reason = 'manual_zombie_recovery',
    last_transition_at = NOW()
WHERE task_id = '<stuck_task_id>'
  AND state = 'Dispatched';
```

### Delete and Recreate Cloud Task

If the Cloud Task itself is stuck:

```bash
# Delete the stuck task
gcloud tasks delete <task-name> --queue=arco-tasks --location=us-central1

# The scheduler will re-dispatch on next tick
```

### Increase Dispatch Ack Timeout

If tasks legitimately take longer to start:

```yaml
# Update scheduler config
apiVersion: v1
kind: ConfigMap
metadata:
  name: arco-scheduler-config
data:
  dispatch_ack_timeout: "120s"  # Increase from 30s
```

## Prevention

1. **Worker Health Checks:**
   - Ensure workers have `/health` endpoint
   - Configure Cloud Run min instances > 0 to avoid cold starts

2. **Heartbeat Reliability:**
   - Use dedicated heartbeat thread
   - Set heartbeat interval < timeout / 3

3. **Monitoring:**
   - Alert on `Dispatched` state duration
   - Track Cloud Tasks delivery latency

4. **Idempotency:**
   - Ensure workers are idempotent
   - Use `task_id + attempt` as deduplication key

## Related Metrics

| Metric | Threshold | Alert |
|--------|-----------|-------|
| `arco_flow_tasks_total{from_state="dispatched", to_state="running"}` rate | < 1/min per worker | Ack slow |
| `arco_flow_task_duration_seconds{state="dispatched"}` p95 | > 60s | Dispatch lag |
| Cloud Tasks `task_attempt_dispatched` count | Increasing while tasks stuck | Retry storm |

## Two-Timeout Model

The scheduler uses two timeouts to detect stuck tasks:

1. **Dispatch-Ack Timeout (30s default):**
   - Time for worker to acknowledge task receipt
   - If exceeded: Task is considered zombie, re-queued

2. **Heartbeat Timeout (60s default):**
   - Time between heartbeats during execution
   - If exceeded: Task is considered zombie, may be retried

```
Dispatched ──(30s)──> Zombie (dispatch-ack timeout)
                      └──> Re-queue for dispatch

Running ──(60s)──> Zombie (heartbeat timeout)
                   └──> Increment attempt, re-dispatch
```

## Escalation

If stuck tasks are blocking critical pipelines:
1. Page on-call
2. Consider marking run as failed to unblock downstream
3. Investigate root cause post-incident
