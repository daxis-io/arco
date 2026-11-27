"""Qualify independent, short-lived Flow pull wakes against the disk fixture."""

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

UTC = dt.timezone.utc
ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
CONTROL = "proof-control-token-0000000000000000"
SESSION = "proof-worker-token-00000000000000000"


def ulid(millis, sequence):
    number = (millis << 80) | sequence
    return "".join(ALPHABET[(number >> shift) & 31] for shift in range(125, -1, -5))


def make_plan(run_id="wake-proof", count=19):
    at = dt.datetime.now(UTC)
    millis = int(at.timestamp() * 1000)
    tasks = [
        {
            "key": f"task-{index:03}",
            "depends_on": [],
            "max_attempts": 1,
            "heartbeat_timeout_sec": 6,
        }
        for index in range(count)
    ]
    run = {
        "type": "run_triggered",
        "run_id": run_id,
        "plan_id": f"plan-{run_id}",
        "root_assets": [],
        "trigger": {"type": "manual", "user_id": "local-proof"},
        "run_key": run_id,
    }
    plan = {
        "type": "plan_created",
        "run_id": run_id,
        "plan_id": f"plan-{run_id}",
        "tasks": tasks,
    }
    events = [
        {
            "event_id": ulid(millis, index),
            "event_type": name,
            "event_version": 1,
            "timestamp": at.isoformat(),
            "source": "arco-flow/proof/wake",
            "tenant_id": "proof",
            "workspace_id": "wake",
            "idempotency_key": f"{key}:{run_id}",
            "correlation_id": run_id,
            "data": data,
        }
        for index, (name, key, data) in enumerate(
            [("RunTriggered", "run", run), ("PlanCreated", "plan", plan)], 1
        )
    ]
    return {
        "version": 1,
        "manifest": None,
        "events": events,
        "taskPayloads": {task["key"]: {"work": task["key"]} for task in tasks},
    }


def selected_manifest(fixture):
    objects = json.loads(fixture.read_text())["objects"]
    pointer_key = next(key for key in objects if key.endswith("/manifest.pointer.json"))
    pointer = json.loads(bytes(objects[pointer_key]["bytes"]))
    prefix = pointer_key.removesuffix("state/orchestration/manifest.pointer.json")
    return json.loads(bytes(objects[prefix + pointer["manifest_path"]]["bytes"]))


def epoch_time(value):
    return dt.datetime.fromisoformat(value).timestamp()


def run_proof(binary, evidence):
    evidence.mkdir(parents=True, exist_ok=False)
    config = evidence / "config.json"
    fixture = evidence / "workspace.json"
    plan_file = evidence / "plan.json"
    config.write_text(json.dumps({
        "tenant": "proof",
        "workspace": "wake",
        "host": {
            "permits": 1,
            "callbackBaseUrl": "http://127.0.0.1:8297",
            "claimKey": [33] * 32,
            "controlToken": CONTROL,
            "sessions": {"worker": SESSION},
        },
    }) + "\n")
    plan = make_plan()
    plan_file.write_text(json.dumps(plan) + "\n")
    timeline = evidence / "invocations.jsonl"

    def invoke(route, payload=None, bearer=CONTROL):
        environment = dict(os.environ, ARCO_PULL_BEARER=bearer)
        with subprocess.Popen(
            [str(binary), str(config), str(fixture), route],
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as process:
            code_input = json.dumps(payload).encode()
            stdout, stderr = process.communicate(code_input, timeout=10)
            row = {
                "pid": process.pid,
                "finishedAt": dt.datetime.now(UTC).isoformat(),
                "route": route,
                "exitCode": process.returncode,
                "stdout": stdout.decode(),
                "stderr": stderr.decode(),
            }
            with timeline.open("a") as log:
                log.write(json.dumps(row) + "\n")
            assert process.returncode == 0, row
            result = json.loads(stdout)
            if route == "accept":
                return result
            assert result["status"] == 200, row
            return result["response"]

    accepted = invoke("accept", plan)
    # The accepting process exits without sending or registering a wake.
    assert [event["event_type"] for event in selected_manifest(fixture)["worker_pull"]["events"]] == [
        "RunTriggered", "PlanCreated",
    ]
    batches = []
    for _ in range(3):
        time.sleep(0.15)
        batches.append(invoke("/run"))
    assert [batch["advanced"] for batch in batches] == [8, 8, 3], batches
    assert [batch["immediateWork"] for batch in batches] == [True, True, False], batches
    assert len({batch["observedRevision"] for batch in batches}) == 3

    def claim():
        return invoke("/api/v1/tasks/claim", {"requestId": "proof-claim"}, SESSION)

    grant = claim()["grant"]
    env = grant["envelope"]
    callback = {
        "attempt": env["attempt"],
        "attemptId": env["attemptId"],
        "workerId": "worker",
    }
    task = env["taskId"]
    assert invoke(f"/api/v1/tasks/{task}/started", callback, env["taskToken"])["acknowledged"]
    old = invoke("/run")["nextDeadline"]
    assert old is not None

    # Later-ready immediate work must not wait for the previously advertised lease hint.
    new_plan = make_plan("arrived-while-permit-held", 1)
    later_plan_file = evidence / "later-plan.json"
    later_plan_file.write_text(json.dumps(new_plan) + "\n")
    invoke("accept", new_plan)
    time.sleep(0.15)
    earlier_work = invoke("/run")
    assert earlier_work["advanced"] == 1 and earlier_work["nextDeadline"] == old, earlier_work
    assert time.time() < epoch_time(old)
    dispatched = selected_manifest(fixture)["worker_pull"]["events"][0]
    assert dispatched["event_type"] == "DispatchRequested"
    assert dispatched["data"]["run_id"] == "arrived-while-permit-held"

    time.sleep(2)
    assert invoke(f"/api/v1/tasks/{task}/heartbeat", callback, env["taskToken"])["acknowledged"]
    renewal = selected_manifest(fixture)
    assert renewal["worker_pull"]["events"][0]["event_type"] == "TaskHeartbeat"
    renewed_deadline = renewal["worker_pull"]["receipts"]["proof-claim"]["deadline"]
    assert epoch_time(renewed_deadline) > epoch_time(old) + 1

    # The external driver has a stale hint. Each invocation reopens the selected scope.
    time.sleep(max(0, epoch_time(old) - time.time() + 0.15))
    stale_hint = invoke("/run")
    assert stale_hint["nextDeadline"] == renewed_deadline, stale_hint
    assert stale_hint["advanced"] == 0, stale_hint
    assert stale_hint["observedRevision"] == renewal["revision_ulid"]
    recovered = None
    remaining = max(0, epoch_time(renewed_deadline) - time.time())
    stop = time.monotonic() + remaining + 4
    while time.monotonic() < stop:
        time.sleep(0.15)
        pulse = invoke("/run")
        if pulse["advanced"]:
            recovered = pulse
            break
    assert recovered and recovered["advanced"] == 1, recovered
    assert recovered["nextDeadline"] is None
    assert not recovered["immediateWork"]
    assert claim()["status"] == "expired"
    final = selected_manifest(fixture)
    assert final["revision_ulid"] == recovered["observedRevision"]
    assert final["worker_pull"]["permits"] == [None]
    assert final["worker_pull"]["events"][0]["event_type"] == "TaskCompletionRecorded"
    assert final["worker_pull"]["events"][0]["data"]["outcome"] == "failed"

    invocations = [json.loads(line) for line in timeline.read_text().splitlines()]
    for invocation in invocations:
        assert invocation["exitCode"] == 0
    assert len({invocation["pid"] for invocation in invocations}) == len(invocations)

    receipt = {
        "acceptedRevision": accepted["observedRevision"],
        "binarySha256": hashlib.file_digest(binary.open("rb"), "sha256").hexdigest(),
        "planSha256": hashlib.file_digest(plan_file.open("rb"), "sha256").hexdigest(),
        "fixtureSha256": hashlib.file_digest(fixture.open("rb"), "sha256").hexdigest(),
        "invocations": len(invocations),
        "backlogBatches": [batch["advanced"] for batch in batches],
        "oldDeadline": old,
        "laterPlanSha256": hashlib.file_digest(later_plan_file.open("rb"), "sha256").hexdigest(),
        "earlierWorkRevision": earlier_work["observedRevision"],
        "renewedDeadline": renewed_deadline,
        "recoveredRevision": recovered["observedRevision"],
        "permitReleased": True,
    }
    (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    run_proof(args.binary.resolve(strict=True), args.evidence)
