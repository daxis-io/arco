"""Run Arco's worker-pull one-shot routes against a fresh loopback MinIO fixture."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
from urllib.error import URLError
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

sys.dont_write_bytecode = True
from qualify import CONTROL, SESSION, make_plan

OTHER = "proof-second-worker-token-000000000000"
SESSIONS = {"worker": SESSION, "other": OTHER}
CLAIM = "/api/v1/tasks/claim"
MINIO_USER = "arco-local-pull"
MINIO_PASSWORD = "arco-local-pull-fixture-password"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def iso(value):
    return datetime.fromisoformat(value).timestamp()


def now():
    return datetime.now(timezone.utc).isoformat()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def wait_until(instant):
    time.sleep(max(0, iso(instant) - time.time() + 0.15))


@contextmanager
def serve_minio(root, minio):
    data = root / "minio-data"
    certs = root / "minio-certs"
    data.mkdir()
    certs.mkdir()
    with socket.socket() as api, socket.socket() as console:
        api.bind(("127.0.0.1", 0))
        console.bind(("127.0.0.1", 0))
        api_address = f"127.0.0.1:{api.getsockname()[1]}"
        console_address = f"127.0.0.1:{console.getsockname()[1]}"
    endpoint = f"http://{api_address}"
    fixture_env = {
        "PATH": os.defpath,
        "AWS_ACCESS_KEY_ID": MINIO_USER,
        "AWS_SECRET_ACCESS_KEY": MINIO_PASSWORD,
        "AWS_ENDPOINT": endpoint,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "AWS_CONFIG_FILE": os.devnull,
    }
    server_env = {
        "PATH": os.defpath,
        "MINIO_ROOT_USER": MINIO_USER,
        "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
        "MINIO_UPDATE": "off",
        "MINIO_BROWSER": "off",
    }
    opener = build_opener(ProxyHandler({}), NoRedirect())
    with (root / "minio.log").open("w") as log:
        with subprocess.Popen(
            [str(minio), "server", "--address", api_address, "--console-address", console_address,
             "--certs-dir", str(certs), str(data)],
            env=server_env, stdout=log, stderr=subprocess.STDOUT,
        ) as server:
            try:
                limit = time.monotonic() + 15
                while time.monotonic() < limit:
                    assert server.poll() is None, "MinIO exited before readiness"
                    try:
                        with opener.open(f"{endpoint}/minio/health/ready", timeout=0.5) as result:
                            assert result.status == 200
                            break
                    except (OSError, URLError):
                        time.sleep(0.05)
                else:
                    raise RuntimeError("loopback MinIO did not become ready")
                yield fixture_env
            finally:
                if server.poll() is None:
                    server.terminate()
                server.wait(timeout=10)


def aws_call(aws, env, *args):
    result = subprocess.run(
        [str(aws), "--region", "us-east-1", "--endpoint-url", env["AWS_ENDPOINT"],
         "--no-cli-pager", "s3api", *args],
        env=env, capture_output=True, text=True, timeout=20, check=False,
    )
    assert result.returncode == 0, {"args": args, "stderr": result.stderr}
    return json.loads(result.stdout) if result.stdout.strip() else {}


class Case:
    def __init__(self, root, binary, aws, env, name, shared_bucket=None, prefix=None):
        self.root = root / name
        self.root.mkdir()
        self.binary = binary
        self.prefix = prefix
        self.env = dict(env, ARCO_PULL_TEST_PREFIX=prefix) if prefix else env
        self.bucket = shared_bucket or f"arco-pull-{name}"
        self.config = self.root / "config.json"
        self.log = self.root / "invocations.jsonl"
        if shared_bucket:
            receipt = {"bucket": self.bucket, "prefix": prefix}
        else:
            receipt = aws_call(aws, env, "create-bucket", "--bucket", self.bucket)
        save(self.root / "bucket.json", receipt)

    def seed(self, task_count=1, permits=1, heartbeat=90, attempts=1, rivers=False):
        save(self.config, {
            "tenant": "proof", "workspace": "wake", "host": {
                "permits": permits, "callbackBaseUrl": "http://127.0.0.1:8297",
                "claimKey": [33] * 32, "controlToken": CONTROL, "sessions": SESSIONS,
            },
        })
        plan = make_plan(count=task_count)
        for task in plan["events"][1]["data"]["tasks"]:
            task["heartbeat_timeout_sec"] = heartbeat
            task["max_attempts"] = attempts
        save(self.root / "plan.json", plan)
        self.invoke("accept-rivers" if rivers else "accept", plan)
        advance = self.invoke("/run")
        assert advance["advanced"] == task_count, advance
        return self.inspect("seeded")

    def spawn(self, route, payload=None, bearer=CONTROL, fault=None):
        environment = dict(self.env, ARCO_PULL_BEARER=bearer)
        marker = None
        if fault:
            mode, name = fault
            marker = self.root / f"{name}.json"
            environment.update(ARCO_PULL_TEST_FAULT=mode, ARCO_PULL_TEST_MARKER=str(marker))
        process = subprocess.Popen(
            [str(self.binary), str(self.config), f"s3://{self.bucket}", route],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=environment,
        )
        return process, route, payload, marker, now()

    @staticmethod
    def send(child):
        process, _, payload, _, _ = child
        process.stdin.write(json.dumps(payload).encode())
        process.stdin.close()
        process.stdin = None

    def command_timeout(self):
        return 12

    def safe_text(self, text):
        return text

    def finish(self, child, expected_exit=0, expected_status=200, include_status=False):
        process, route, _payload, marker, started = child
        try:
            stdout, stderr = process.communicate(timeout=self.command_timeout())
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
        row = {
            "pid": process.pid, "route": route, "startedAt": started, "finishedAt": now(),
             "exitCode": process.returncode, "stdout": self.safe_text(stdout.decode(errors="replace")),
            "stderr": self.safe_text(stderr.decode(errors="replace")),
            "faultMarker": marker.name if marker else None,
        }
        with self.log.open("a") as stream:
            stream.write(json.dumps(row) + "\n")
        assert process.returncode == expected_exit, row
        if expected_exit:
            assert not stdout, row
            return None
        result = json.loads(stdout)
        if "status" in result:
            allowed = (expected_status,) if isinstance(expected_status, int) else expected_status
            assert result["status"] in allowed, row
            return result if include_status else result["response"]
        return result

    def invoke(self, route, payload=None, bearer=CONTROL, **options):
        fault = options.pop("fault", None)
        child = self.spawn(route, payload, bearer, fault)
        self.send(child)
        return self.finish(child, **options)

    def pause(self, route, payload, bearer, mode, name):
        child = self.spawn(route, payload, bearer, (mode, name))
        process, _, _, marker, _ = child
        self.send(child)
        limit = time.monotonic() + 4
        while not marker.is_file() and process.poll() is None and time.monotonic() < limit:
            time.sleep(0.01)
        if not marker.is_file() or process.poll() is not None:
            self.finish(child)
            raise AssertionError(f"child failed to stop at {name}")
        observed = json.loads(marker.read_text())
        assert observed["pid"] == process.pid
        return child, observed

    def crash(self, route, payload, bearer, mode, name):
        child, observed = self.pause(route, payload, bearer, mode, name)
        child[0].kill()
        self.finish(child, expected_exit=-signal.SIGKILL)
        return observed

    def inspect(self, label):
        result = self.invoke("inspect")
        save(self.root / f"state-{label}.json", result)
        return result

    def claim(self, worker, request, **options):
        return self.invoke(CLAIM, {"requestId": request}, SESSIONS[worker], **options)

    def race(self, first, second, **options):
        children = [self.spawn(CLAIM, {"requestId": request}, SESSIONS[worker])
                    for worker, request in [first, second]]
        assert len({child[0].pid for child in children}) == 2
        assert all(child[0].poll() is None for child in children)
        for child in children:
            self.send(child)
        return [self.finish(child, **options) for child in children]


def pull(state):
    return state["manifest"]["worker_pull"]


def callbacks(grant, worker, kind, **fields):
    envelope = grant["envelope"]
    route = f"/api/v1/tasks/{envelope['taskId']}/{kind}"
    body = {"attempt": envelope["attempt"], "attemptId": envelope["attemptId"], "workerId": worker}
    body.update(fields)
    return route, body, envelope["taskToken"]


def fact(case, grant, worker, kind, expected_status=200, fault=None, **fields):
    route, body, token = callbacks(grant, worker, kind, **fields)
    return case.invoke(route, body, token, expected_status=expected_status, fault=fault)


def granted(reply):
    assert reply["status"] == "granted", reply
    return reply["grant"]


def same_attempt(old_dispatch, grant):
    envelope = grant["envelope"]
    assert envelope["dispatchId"] == old_dispatch["dispatch_id"]
    assert envelope["attemptId"] == old_dispatch["attempt_id"]
    assert envelope["attempt"] == old_dispatch["attempt"]


def check_cas(before, after, marker, committed):
    assert marker["expectedVersion"] == before["pointerVersion"]
    assert marker["pointer"]["epoch"] == after["lock"]["sequenceNumber"]
    assert marker["key"].endswith("/state/orchestration/manifest.pointer.json")
    if committed:
        assert marker["stage"] == "after"
        assert marker["storedVersion"] == after["pointerVersion"] != before["pointerVersion"]
        assert marker["pointer"] == after["pointer"]
        assert after["manifest"]["previous_manifest_path"] == before["pointer"]["manifest_path"]
    else:
        assert marker["stage"] == "before" and marker["storedVersion"] is None
        assert after["pointerVersion"] == before["pointerVersion"]
        assert after["pointer"] == before["pointer"]
        assert marker["pointer"]["manifest_id"] != after["manifest"]["manifest_id"]


def await_lock_expiry(case, locked):
    lock = locked["lock"]
    assert abs(iso(lock["expiresAt"]) - iso(lock["acquiredAt"]) - 30) < 0.001
    assert time.time() < iso(lock["expiresAt"]), lock
    wait_until(lock["expiresAt"])
    expired = case.inspect("natural-lock-expiry")
    assert expired["lock"] == lock
    assert time.time() > iso(expired["lock"]["expiresAt"])
    assert expired["pointerVersion"] == locked["pointerVersion"]
    return lock["sequenceNumber"]


def run_succeeded(state, count):
    run = state["runs"]["wake-proof"]
    assert (run["state"], run["tasks_completed"], run["tasks_succeeded"]) == ("SUCCEEDED", count, count)
    assert not any(pull(state)["permits"])


def complete(case, grant, worker):
    assert fact(case, grant, worker, "started")["acknowledged"]
    assert fact(case, grant, worker, "completed", outcome="SUCCEEDED")["finalState"] == "SUCCEEDED"


def competing_claims(case):
    case.seed()
    identities = [("worker", "first"), ("other", "second")]
    replies = case.race(*identities)
    assert sorted(reply["status"] for reply in replies) == ["busy", "granted"]
    index = next(index for index, reply in enumerate(replies) if reply["status"] == "granted")
    worker, request = identities[index]
    opponent, losing_request = identities[1 - index]
    grant = granted(replies[index])
    started = fact(case, grant, worker, "started")
    assert started["acknowledged"]
    assert fact(case, grant, worker, "started") == started

    active = granted(case.claim(worker, request))
    assert {**active, "leaseExpiresAt": grant["leaseExpiresAt"]} == grant
    assert iso(active["leaseExpiresAt"]) > iso(grant["leaseExpiresAt"])
    assert case.claim(opponent, losing_request)["status"] == "busy"
    case.claim(opponent, request, expected_status=409)
    state = case.inspect("winner")
    authority = pull(state)
    assert authority["permits"] == [grant["generation"]]
    assert list(authority["receipts"]) == [request]
    receipt = authority["receipts"][request]
    assert receipt["session"] == worker and receipt["generation"] == grant["generation"]
    assert receipt["permit"] == grant["permit"]
    assert receipt["started_at"] == started["serverTime"] and receipt["deadline"] == active["leaseExpiresAt"]
    task, = state["tasks"]
    assert task["state"] == "RUNNING"
    assert (task["attempt"], task["attempt_id"]) == (
        grant["envelope"]["attempt"], grant["envelope"]["attemptId"],
    )
    assert fact(case, grant, worker, "completed", outcome="SUCCEEDED")["finalState"] == "SUCCEEDED"
    run_succeeded(case.inspect("terminal"), 1)
    return {"winner": request, "generation": grant["generation"]}


def crash_before(case):
    initial = case.seed()
    marker = case.crash(CLAIM, {"requestId": "same"}, SESSION, "stop-before", "before-cas")
    locked = case.inspect("after-kill")
    check_cas(initial, locked, marker, False)
    assert pull(locked)["permits"] == [None] and not pull(locked)["receipts"]
    assert len(locked["dispatches"]) == 1
    candidate = case.invoke("snapshot:" + marker["pointer"]["manifest_id"])
    save(case.root / "prepared-snapshot.json", candidate)
    orphan = candidate["worker_pull"]["receipts"]["same"]
    assert candidate["previous_manifest_path"] == initial["pointer"]["manifest_path"]
    case.invoke("/run", expected_status=503)
    fence = await_lock_expiry(case, locked)
    recovered = granted(case.claim("worker", "same"))
    same_attempt(orphan["dispatch"], recovered)
    assert recovered["generation"] > fence == orphan["generation"]
    winner = case.inspect("recovered")
    assert winner["manifest"]["previous_manifest_path"] == initial["pointer"]["manifest_path"]
    assert granted(case.claim("worker", "same")) == recovered
    complete(case, recovered, "worker")
    run_succeeded(case.inspect("terminal"), 1)
    return {"orphanGeneration": fence, "recoveredGeneration": recovered["generation"]}


def crash_after(case):
    initial = case.seed()
    marker = case.crash(CLAIM, {"requestId": "same"}, SESSION, "stop-after", "after-cas")
    locked = case.inspect("after-kill")
    check_cas(initial, locked, marker, True)
    original = pull(locked)["receipts"]["same"]
    assert pull(locked)["permits"] == [original["generation"]]
    assert iso(original["start_before"]) < iso(locked["lock"]["expiresAt"])
    fence = await_lock_expiry(case, locked)
    assert case.claim("worker", "same")["status"] == "expired"
    expired = case.inspect("expired")
    assert pull(expired)["permits"] == [None]
    assert pull(expired)["receipts"]["same"]["generation"] == fence
    assert expired["manifest"]["previous_manifest_path"] == locked["pointer"]["manifest_path"]
    recovered = granted(case.claim("other", "fresh"))
    same_attempt(original["dispatch"], recovered)
    assert recovered["generation"] > fence
    assert case.claim("worker", "same")["status"] == "superseded"
    assert granted(case.claim("other", "fresh")) == recovered
    complete(case, recovered, "other")
    run_succeeded(case.inspect("terminal"), 1)
    return {"expiredGeneration": fence, "replacementGeneration": recovered["generation"]}


def reply_loss(case):
    before = case.seed(task_count=2)
    case.claim("worker", "original", expected_status=503, fault=("lose-after", "lost-grant"))
    selected = case.inspect("accepted-grant")
    check_cas(before, selected, json.loads((case.root / "lost-grant.json").read_text()), True)
    grant = granted(case.claim("worker", "original"))
    assert grant["generation"] == pull(selected)["receipts"]["original"]["generation"]

    for kind in ["started", "heartbeat", "completed"]:
        before = selected
        fields = {"outcome": "SUCCEEDED"} if kind == "completed" else {}
        fact(case, grant, "worker", kind, expected_status=503, fault=("lose-after", f"lost-{kind}"), **fields)
        selected = case.inspect(f"accepted-{kind}")
        marker = json.loads((case.root / f"lost-{kind}.json").read_text())
        check_cas(before, selected, marker, True)
        receipt = pull(selected)["receipts"]["original"]
        if kind == "started":
            replay = fact(case, grant, "worker", kind)
            assert replay["serverTime"] == receipt["started_at"]
            assert len(selected["dispatches"]) == 1
            assert case.claim("other", "replacement")["status"] == "busy"
        if kind == "heartbeat":
            replay = granted(case.claim("worker", "original"))
            assert replay["leaseExpiresAt"] == receipt["deadline"]
            assert iso(replay["leaseExpiresAt"]) > iso(pull(before)["receipts"]["original"]["deadline"])
            assert replay["envelope"] == grant["envelope"]
        if kind == "completed":
            assert pull(selected)["permits"] == [None]
            assert selected["runs"]["wake-proof"]["tasks_completed"] == 1

    replacement = granted(case.claim("other", "replacement"))
    assert fact(case, replacement, "other", "started")["acknowledged"]
    assert replacement["permit"] == grant["permit"]
    assert replacement["generation"] > grant["generation"]
    replay = fact(case, grant, "worker", "completed", outcome="SUCCEEDED")
    stored = case.claim("worker", "original")
    assert (stored["status"], stored["finalState"], stored["acceptedAt"]) == (
        "completed", "SUCCEEDED", replay["serverTime"])
    fact(case, grant, "worker", "completed", expected_status=409, outcome="FAILED")
    fact(case, grant, "worker", "heartbeat", expected_status=409)
    assert pull(case.inspect("replacement-held"))["permits"] == [replacement["generation"]]
    assert fact(case, replacement, "other", "completed", outcome="SUCCEEDED")["finalState"] == "SUCCEEDED"
    run_succeeded(case.inspect("terminal"), 2)
    return {"generation": grant["generation"], "acceptedCompletionTime": stored["acceptedAt"]}


def two_permit_restart(case):
    case.seed(task_count=3, permits=2)
    first, second = map(granted, case.race(("worker", "first"), ("other", "second")))
    assert first["permit"] != second["permit"]
    assert first["envelope"]["taskId"] != second["envelope"]["taskId"]
    fact(case, first, "worker", "started")
    fact(case, second, "other", "started")
    assert case.claim("worker", "third")["status"] == "busy"
    before = case.inspect("both-running")
    assert sorted(pull(before)["permits"]) == sorted([first["generation"], second["generation"]])
    assert [task["state"] for task in before["tasks"]].count("RUNNING") == 2

    route, body, token = callbacks(first, "worker", "heartbeat")
    marker = case.crash(route, body, token, "stop-after", "heartbeat-cas")
    locked = case.inspect("after-kill")
    check_cas(before, locked, marker, True)
    assert pull(locked)["permits"] == pull(before)["permits"]
    assert locked["runs"]["wake-proof"]["tasks_completed"] == 0
    assert locked["manifest"]["worker_pull"]["events"][0]["event_type"] == "TaskHeartbeat"
    await_lock_expiry(case, locked)
    assert case.claim("worker", "third")["status"] == "busy"
    first_replay = granted(case.claim("worker", "first"))
    assert first_replay["generation"] == first["generation"]
    assert first_replay["leaseExpiresAt"] == pull(locked)["receipts"]["first"]["deadline"]
    second_replay = granted(case.claim("other", "second"))
    assert second_replay["generation"] == second["generation"]
    assert second_replay["envelope"] == second["envelope"]
    fact(case, first, "worker", "completed", outcome="SUCCEEDED")

    third = granted(case.claim("worker", "third"))
    assert third["permit"] == first["permit"] and third["generation"] > first["generation"]
    replay = granted(case.claim("other", "second"))
    assert replay["generation"] == second["generation"] and replay["envelope"] == second["envelope"]
    completed = case.claim("worker", "first")
    assert completed["status"] == "completed" and completed["generation"] == first["generation"]
    assert fact(case, first, "worker", "completed", outcome="SUCCEEDED")["serverTime"] == completed["acceptedAt"]
    fact(case, first, "worker", "heartbeat", expected_status=409)
    fact(case, first, "worker", "started", expected_status=409)
    assert case.claim("worker", "fourth")["status"] == "busy"
    state = case.inspect("two-permits-after-restart")
    assert sorted(pull(state)["permits"]) == sorted([second["generation"], third["generation"]])
    assert state["runs"]["wake-proof"]["tasks_completed"] == 1
    fact(case, second, "other", "completed", outcome="SUCCEEDED")
    complete(case, third, "worker")
    run_succeeded(case.inspect("terminal"), 3)
    return {"preservedGeneration": second["generation"], "reassignedGeneration": third["generation"]}


def rivers_retry(case):
    seeded = case.seed(attempts=2, rivers=True)
    run_id = "wake-proof"
    accepted = {run_id: pull(seeded)["accepted_runs"][run_id]}
    policy = "rivers-d7cbc206-compat-v1"
    labels = seeded["runs"][run_id]["labels"]
    assert labels["arco.retry_policy"] == accepted[run_id]["retry_policy"] == policy
    assert labels["arco.accepted_plan_sha256"] == accepted[run_id]["sha256"]

    original = granted(case.claim("worker", "original"))
    assert original["envelope"]["attempt"] == 1
    assert fact(case, original, "worker", "started")["acknowledged"]
    before = case.inspect("before-failed-completion")
    failure = {"outcome": "FAILED", "error": {
        "category": "INFRASTRUCTURE", "message": "synthetic transient failure", "retryable": True,
    }}
    fact(case, original, "worker", "completed", expected_status=503,
         fault=("lose-after", "lost-completed"), **failure)
    marker = json.loads((case.root / "lost-completed.json").read_text())
    selected = case.inspect("retry-wait")
    check_cas(before, selected, marker, True)
    authority = pull(selected)
    assert authority["accepted_runs"] == accepted and authority["permits"] == [None]
    receipt = authority["receipts"]["original"]
    assert receipt["phase"] == "Completed" and receipt["generation"] == original["generation"]
    completion = receipt["completion"]
    assert completion["state"] == "FAILED"
    task, = selected["tasks"]
    assert task["state"] == "RETRY_WAIT" and task["attempt"] == 1
    assert task["attempt_id"] == original["envelope"]["attemptId"]
    assert not selected["dispatches"]
    due = task["retry_not_before"]
    assert abs(iso(due) - iso(completion["accepted_at"]) - 30) < 0.01

    replay = fact(case, original, "worker", "completed", **failure)
    replay_process = json.loads(case.log.read_text().splitlines()[-1])["pid"]
    assert replay_process != marker["pid"]
    assert replay == {"acknowledged": True, "finalState": "FAILED",
                      "serverTime": completion["accepted_at"]}
    early = case.claim("other", "early")
    assert early["status"] == "empty" and time.time() < iso(due), early
    before_due = case.invoke("/run")
    assert before_due["advanced"] == 0 and not before_due["immediateWork"]
    assert before_due["observedRevision"] == selected["manifest"]["revision_ulid"]
    assert before_due["nextDeadline"] == due and time.time() < iso(due)
    wait_until(due)
    assert case.invoke("/run")["advanced"] == 1

    contenders = [("worker", "retry-first"), ("other", "retry-second")]
    replies = case.race(*contenders)
    assert sorted(reply["status"] for reply in replies) == ["busy", "granted"]
    winner = next(i for i, reply in enumerate(replies) if reply["status"] == "granted")
    worker, request = contenders[winner]
    replacement = granted(replies[winner])
    first, second = original["envelope"], replacement["envelope"]
    assert (second["runId"], second["taskKey"], second["attempt"]) == (run_id, first["taskKey"], 2)
    assert second["attemptId"] != first["attemptId"] and second["dispatchId"] != first["dispatchId"]
    assert replacement["permit"] == original["permit"] and replacement["generation"] > original["generation"]
    assert fact(case, replacement, worker, "started")["acknowledged"]
    fact(case, original, "worker", "heartbeat", expected_status=409)
    fact(case, original, "worker", "completed", expected_status=409, outcome="SUCCEEDED")

    held = pull(case.inspect("retry-held"))
    assert held["permits"] == [replacement["generation"]]
    assert set(held["receipts"]) == {"original", request} and held["receipts"]["original"] == receipt
    assert fact(case, replacement, worker, "completed", outcome="SUCCEEDED")["finalState"] == "SUCCEEDED"
    assert case.claim("other", "after-success")["status"] == "empty"
    terminal = case.inspect("terminal")
    run_succeeded(terminal, 1)
    final = pull(terminal)
    assert final["accepted_runs"] == accepted
    assert set(final["receipts"]) == {"original", request}
    assert final["receipts"]["original"] == receipt
    assert final["receipts"][request]["phase"] == "Completed"
    assert final["receipts"][request]["completion"]["state"] == "SUCCEEDED"
    return {"policy": policy, "acceptedPlan": accepted[run_id], "retryNotBefore": due,
            "originalGeneration": original["generation"], "retryGeneration": replacement["generation"],
            "completionProcess": marker["pid"], "replayProcess": replay_process}


def running_expiry(case):
    case.seed(heartbeat=3, attempts=2)
    original = granted(case.claim("worker", "first"))
    fact(case, original, "worker", "started")
    running = case.inspect("started")
    wait_until(pull(running)["receipts"]["first"]["deadline"])
    assert case.claim("other", "retry")["status"] == "empty"
    waiting = case.inspect("retry-wait")
    task = waiting["tasks"][0]
    assert task["state"] == "RETRY_WAIT" and task["attempt"] == 1
    assert pull(waiting)["permits"] == [None]
    assert case.invoke("/run")["nextDeadline"] == task["retry_not_before"]
    fact(case, original, "worker", "completed", expected_status=409, outcome="SUCCEEDED")
    wait_until(task["retry_not_before"])
    retry = granted(case.claim("other", "retry"))
    assert retry["envelope"]["attempt"] == 2
    assert retry["envelope"]["attemptId"] != original["envelope"]["attemptId"]
    assert retry["generation"] > original["generation"]
    complete(case, retry, "other")
    run_succeeded(case.inspect("terminal"), 1)
    return {"attempt": 2, "retryNotBefore": task["retry_not_before"]}


def stale_cas_loses(case):
    before = case.seed()
    child, marker = case.pause(CLAIM, {"requestId": "old"}, SESSION, "hold-before", "stale")
    try:
        broken = case.invoke("break-fixture-lock")["lock"]
        assert broken["operation"] == "force-break"
        assert broken["sequenceNumber"] == marker["pointer"]["epoch"]
        assert marker["expectedVersion"] == before["pointerVersion"]
        winner = granted(case.claim("other", "winner"))
        selected = case.inspect("winner-before-stale-cas")
        assert winner["generation"] > marker["pointer"]["epoch"]
    finally:
        child[3].with_suffix(".resume").touch()
    assert case.finish(child)["status"] == "busy"
    failure = json.loads((case.root / "stale.result.json").read_text())
    assert failure == {"currentVersion": selected["pointerVersion"]}
    after = case.inspect("winner-after-stale-cas")
    assert selected["pointer"] == after["pointer"]
    assert selected["pointerVersion"] == after["pointerVersion"]
    assert pull(after)["permits"] == [winner["generation"]]
    assert list(pull(after)["receipts"]) == ["winner"]
    assert granted(case.claim("other", "winner")) == winner
    complete(case, winner, "other")
    run_succeeded(case.inspect("terminal"), 1)
    return {"abandonedGeneration": marker["pointer"]["epoch"], "winnerGeneration": winner["generation"]}


def versioned_bucket(aws, env):
    bucket = "arco-pull-versioned"
    aws_call(aws, env, "create-bucket", "--bucket", bucket)
    aws_call(aws, env, "put-bucket-versioning", "--bucket", bucket,
             "--versioning-configuration", "Status=Enabled")
    publication = aws_call(aws, env, "get-bucket-versioning", "--bucket", bucket)
    assert publication["Status"] == "Enabled"
    return bucket, publication


def verify_versioned_prefixes(root, aws, env, bucket, cases):
    versions = aws_call(aws, env, "list-object-versions", "--bucket", bucket, "--no-paginate")
    save(root / "object-versions.json", versions)
    assert not versions.get("IsTruncated")
    entries = versions["Versions"]
    assert not versions.get("DeleteMarkers")
    assert all(any(entry["Key"].startswith(case.prefix + "/") for case in cases.values())
               for entry in entries), "an object escaped the physical prefixes"
    observed = {}
    for name, case in cases.items():
        state = case.inspect("versioned-metadata")
        latest = [entry for entry in entries if entry["IsLatest"]
                  and entry["Key"].startswith(case.prefix + "/")]
        lock_key = case.prefix + "/locks/orchestration.compaction.lock.json"
        pointer = next(entry for entry in latest if entry["Key"].endswith("/manifest.pointer.json"))
        evidence = {"prefix": case.prefix, "lockKey": lock_key, "pointerKey": pointer["Key"]}
        for kind, key in [("lock", lock_key), ("pointer", pointer["Key"])]:
            matching = [entry for entry in entries if entry["Key"] == key]
            assert len(matching) >= 2 and sum(entry["IsLatest"] for entry in matching) == 1
            head = aws_call(aws, env, "head-object", "--bucket", bucket, "--key", key)
            assert head["VersionId"] != "null" and head["ETag"]
            token = json.loads(state[f"{kind}Version"])
            assert token == {"version": head["VersionId"], "e_tag": head["ETag"]}
            assert state[f"{kind}Etag"] == head["ETag"]
            evidence[kind] = {"versionId": head["VersionId"], "etag": head["ETag"],
                              "retainedVersions": len(matching)}
        save(case.root / "physical-identity.json", evidence)
        observed[name] = evidence
    return {"objectVersions": len(entries), "prefixes": observed}


SCENARIOS = {
    "race": competing_claims,
    "before-cas": crash_before,
    "after-cas": crash_after,
    "reply-loss": reply_loss,
    "two-permits": two_permit_restart,
    "running-expiry": running_expiry,
    "rivers-retry": rivers_retry,
    "stale-cas": stale_cas_loses,
}


def main():
    if sys.flags.optimize:
        raise SystemExit("qualification requires assertions enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--case", choices=SCENARIOS, action="append")
    parser.add_argument("--versioned-prefix", action="store_true")
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = args.output.absolute()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    minio = shutil.which("minio")
    aws = shutil.which("aws")
    assert minio and aws, "minio and aws must be installed"
    info = {
        "file": str(binary), "binarySha256": sha256(binary),
        "minioSha256": sha256(Path(minio)),
        "minioVersion": subprocess.check_output([minio, "--version"], text=True).splitlines()[0],
        "awsVersion": subprocess.check_output([aws, "--version"], text=True).strip(),
        "cases": {},
    }
    try:
        names = args.case or list(SCENARIOS)
        with serve_minio(root, minio) as env:
            if args.versioned_prefix:
                bucket, config = versioned_bucket(aws, env)
                prefix = f"conformance/s3/worker-pull-{os.getpid()}-{time.time_ns()}"
                info["versioning"] = {"bucket": bucket, "configuration": config}
                cases = {name: Case(root, binary, aws, env, name, bucket, f"{prefix}/{name}")
                         for name in names}
            else:
                cases = {name: Case(root, binary, aws, env, name) for name in names}
            with ThreadPoolExecutor(max_workers=len(cases)) as executor:
                futures = {name: executor.submit(SCENARIOS[name], case) for name, case in cases.items()}
                for name, future in futures.items():
                    try:
                        info["cases"][name] = {"passed": True, **future.result()}
                    except Exception:
                        info["cases"][name] = {"passed": False, "error": traceback.format_exc()}
            info["passed"] = all(result["passed"] for result in info["cases"].values())
            if info["passed"] and args.versioned_prefix:
                info["versioning"].update(verify_versioned_prefixes(root, aws, env, bucket, cases))
    except Exception:
        info["passed"] = False
        info["error"] = traceback.format_exc()
    finally:
        info["sourceSha256"] = sha256(Path(__file__))
        save(root / "receipt.json", info)
    print(json.dumps(info, indent=2))
    return 0 if info["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
