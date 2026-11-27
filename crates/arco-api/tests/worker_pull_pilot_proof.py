"""Qualify the normal Arco API worker-pull pilot against versioned S3."""

import argparse
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request, build_opener

sys.dont_write_bytecode = True
if not Path(__file__).with_name("qualify_minio.py").is_file():
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]
                           / "arco-flow/examples/operator_owned_worker_pull"))
from qualify_hosted_s3 import BUCKET, OWNER, REGION, redact, session_environment
from qualify_minio import NoRedirect, aws_call, serve_minio, sha256, versioned_bucket


TENANT, WORKSPACE = "proof", "wake"
BASE = f"/api/v1/workspaces/{WORKSPACE}"
PULL = "/worker-pull"
WAKE = PULL + "/run"
CLAIM = PULL + "/api/v1/tasks/claim"
RETRY_POLICY = "rivers-d7cbc206-compat-v1"
WAKE_INTERVAL = 8
OPENER = build_opener(ProxyHandler({}), NoRedirect())


def utc():
    return datetime.now(timezone.utc).isoformat()


def read(path):
    return json.loads(path.read_text())


def save(path, data):
    with path.open("x", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")


def record(log, data):
    with log.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(data, separators=(",", ":")) + "\n")


def records(log):
    if not log.is_file():
        return []
    return [json.loads(line) for line in log.read_text().splitlines(keepends=True)
            if line.endswith("\n")]


def request(origin, path, token=None, body=None, expected=200, log=None, method=None):
    started = utc()
    tick = time.monotonic()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    query = Request(origin + path, data=None if body is None else json.dumps(body).encode(),
                    headers=headers, method=method)
    try:
        with OPENER.open(query, timeout=15) as response:
            status, content = response.status, response.read()
    except HTTPError as error:
        status, content = error.code, error.read()
    result = json.loads(content) if content else None
    entry = {"path": path, "httpStatus": status, "startedAt": started, "finishedAt": utc(),
             "wallMs": round((time.monotonic() - tick) * 1000, 2)}
    if log is not None:
        record(log, entry)
    allowed = (expected,) if isinstance(expected, int) else expected
    if status not in allowed:
        detail = result.get("error") if isinstance(result, dict) else None
        raise AssertionError(f"{path} returned {status}, expected {expected}, error {detail!r}")
    return result, entry


def eventually(description, check, timeout=36):
    limit = time.monotonic() + timeout
    while time.monotonic() < limit:
        result = check()
        if result:
            return result
        time.sleep(0.15)
    raise RuntimeError(f"Timed out waiting for {description}")


def ready(origin, process):
    def healthy():
        if process.poll() is not None:
            raise RuntimeError(f"Arco API exited with status {process.returncode}")
        try:
            request(origin, "/health")
        except (URLError, TimeoutError):
            return False
        return True
    eventually("Arco API readiness", healthy, timeout=15)


def stop(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=7)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def scheduler(root, binary, origin):
    """Run independently of the client, including while the API is down."""
    def shutdown(_signum, _frame):
        raise SystemExit(1)
    signal.signal(signal.SIGTERM, shutdown)
    control = read(root / "pilot.json")["host"]["controlToken"]
    log = root / "scheduler.jsonl"
    api = None
    wake_at = time.monotonic()
    end = wake_at + 240
    instance = 0
    crashed = False
    try:
        while not (root / "stop").exists() and time.monotonic() < end:
            crash = root / "restart.json"
            if not crashed and crash.is_file():
                expected = read(crash)
                assert api is not None and api.poll() is None and api.pid == expected["pid"]
                api.kill()
                code = api.wait(timeout=5)
                assert code == -9, code
                record(log, {"event": "killed", "pid": api.pid, "exit": code, "at": utc()})
                api = None
                crashed = True
            if time.monotonic() < wake_at:
                time.sleep(0.1)
                continue
            if api is None:
                instance += 1
                with (root / f"api-{instance}.log").open("xb") as output:
                    api = subprocess.Popen([str(binary)], stdout=output, stderr=subprocess.STDOUT)
                ready(origin, api)
                record(log, {"event": "started", "pid": api.pid, "at": utc()})
            if api.poll() is not None:
                raise RuntimeError(f"Arco API unexpectedly exited with status {api.returncode}")
            reply, trace = request(origin, WAKE, control, {}, expected=(200, 503))
            record(log, {"event": "wake", "pid": api.pid, "schedulerPid": os.getpid(),
                         **trace, "response": reply})
            wake_at = time.monotonic() + WAKE_INTERVAL
        if not (root / "stop").exists():
            raise RuntimeError("Recurring scheduler exhausted the proof time budget")
    finally:
        stop(api)


def jwt(secret, workspace=WORKSPACE):
    def encoded(data):
        return base64.urlsafe_b64encode(data).rstrip(b"=")
    header = encoded(b'{"alg":"HS256","typ":"JWT"}')
    claims = {"tenant": TENANT, "workspace": workspace, "sub": "worker-pull-proof",
              "iss": "api-pilot", "aud": "arco-api", "exp": int(time.time()) + 600}
    body = header + b"." + encoded(json.dumps(claims).encode())
    return (body + b"." + encoded(hmac.digest(secret.encode(), body, "sha256"))).decode()


def manifest():
    return {"manifestVersion": "1.0", "codeVersionId": "pilot-v1", "assets": [
        {"key": {"namespace": "proof", "name": name},
         "id": f"00000000-0000-4000-8000-{index:012}",
         "execution": {"maxAttempts": 2, "heartbeatTimeoutSec": 120, "payload": {"value": index}}}
        for index, name in enumerate(("alpha", "beta", "gamma"), 1)
    ]}


def exercise(root, process, origin, host, user_secret):
    log = root / "client.jsonl"
    schedule = root / "scheduler.jsonl"
    user = jwt(user_secret)

    def http(path, token=None, body=None, expected=200):
        return request(origin, path, token, body, expected, log)[0]

    def observed(label, predicate, timeout=36):
        def find():
            if process.poll() is not None:
                raise RuntimeError("Scheduler exited early. See scheduler.log")
            return next((entry for entry in records(schedule) if predicate(entry)), None)
        return eventually(label, find, timeout)

    def claim(session, request_id, expected="granted"):
        response = http(CLAIM, host["sessions"][session], {"requestId": request_id})
        assert response["status"] == expected, response["status"]
        if expected == "granted":
            grant = response["grant"]
            assert grant["envelope"]["callbackBaseUrl"] == host["callbackBaseUrl"]
            return grant
        return None

    def callback(grant, session, action, fields=None, expected=200):
        envelope = grant["envelope"]
        body = {"attempt": envelope["attempt"], "attemptId": envelope["attemptId"],
                "workerId": session, **(fields or {})}
        path = PULL + "/api/v1/tasks/" + quote(envelope["taskId"], safe="") + "/" + action
        return http(path, envelope["taskToken"], body, expected)

    first_start = observed("initial API start", lambda e: e["event"] == "started")
    http(BASE + "/manifests", user, manifest(), 201)
    body = {"selection": ["proof/alpha", "proof/beta", "proof/gamma"], "runKey": "pilot-stable"}
    http(BASE + "/runs", jwt(user_secret, "other"), body, 404)
    http(CLAIM, "invalid-session", {"requestId": "unauthenticated"}, 401)
    http(WAKE, host["sessions"]["left"], {}, 401)
    first = http(BASE + "/runs", user, body, 201)
    run_path = BASE + "/runs/" + first["runId"]
    seeded = http(run_path, user)
    assert seeded["labels"]["arco.retry_policy"] == RETRY_POLICY
    plan_sha = seeded["labels"]["arco.accepted_plan_sha256"]
    replay = http(BASE + "/runs", user, body)
    assert (replay["runId"], replay["planId"], replay["created"]) == (first["runId"], first["planId"], False)
    http(BASE + "/runs", user, {**body, "selection": ["proof/beta"]}, 409)
    observed("initial scheduled dispatch", lambda e: e["event"] == "wake"
             and e["httpStatus"] == 200 and e["response"]["advanced"] == 3)

    left = claim("left", "first")
    assert callback(left, "left", "started")["acknowledged"]
    right = claim("right", "second")
    assert callback(right, "right", "started")["acknowledged"]
    assert {left["permit"], right["permit"]} == {0, 1}
    claim("right", "backlog", "busy")
    success = {"outcome": "SUCCEEDED"}
    assert callback(right, "right", "completed", success)["finalState"] == "SUCCEEDED"
    backlog = claim("right", "backlog")
    assert backlog["permit"] == right["permit"]
    assert callback(backlog, "right", "started")["acknowledged"]
    assert callback(backlog, "right", "completed", success)["finalState"] == "SUCCEEDED"

    failure = {"outcome": "FAILED", "error": {"category": "INFRASTRUCTURE",
               "message": "synthetic retryable failure", "retryable": True}}
    failed = callback(left, "left", "completed", failure)
    assert failed["finalState"] == "FAILED"
    due = datetime.fromisoformat(failed["serverTime"]) + timedelta(seconds=30)
    save(root / "restart.json", first_start)
    killed = observed("API process kill", lambda e: e["event"] == "killed")
    assert datetime.fromisoformat(killed["at"]) < due
    restarted = observed("scheduled API restart",
                         lambda e: e["event"] == "started" and e["pid"] != first_start["pid"])
    # A scheduler tick in the new process recovers the committed retry wait.
    before = observed("new host seeing the future retry", lambda e: e["event"] == "wake"
                      and e["pid"] == restarted["pid"] and e["httpStatus"] == 200
                      and e["response"].get("nextDeadline") is not None
                      and e["response"]["advanced"] == 0)
    persisted_due = datetime.fromisoformat(before["response"]["nextDeadline"])
    assert abs((due - persisted_due).total_seconds()) < 0.01
    assert datetime.fromisoformat(before["finishedAt"]) < persisted_due
    replay = http(BASE + "/runs", user, body)
    assert (replay["runId"], replay["planId"], replay["created"]) == (first["runId"], first["planId"], False)
    waiting = http(run_path, user)
    assert waiting["labels"]["arco.accepted_plan_sha256"] == plan_sha
    assert all(task["attempt"] == 1 for task in waiting["tasks"])
    assert callback(left, "left", "completed", failure) == failed
    wake = observed("scheduled retry advancement", lambda e: e["event"] == "wake"
                    and e["pid"] == restarted["pid"] and e["httpStatus"] == 200
                    and e["response"]["advanced"] > 0, timeout=52)
    assert wake["response"]["advanced"] == 1
    assert datetime.fromisoformat(wake["startedAt"]) >= persisted_due
    for event in records(log):
        if event["path"] == CLAIM:
            assert datetime.fromisoformat(event["startedAt"]) < datetime.fromisoformat(killed["at"])

    grant = claim("right", "retry")
    envelope, earlier = grant["envelope"], left["envelope"]
    assert (envelope["runId"], envelope["taskKey"], envelope["attempt"]) == (first["runId"], earlier["taskKey"], 2)
    assert envelope["attemptId"] != earlier["attemptId"] and grant["generation"] > left["generation"]
    assert envelope["payload"] == earlier["payload"]
    assert callback(grant, "right", "started")["acknowledged"]
    callback(left, "left", "heartbeat", expected=409)
    assert callback(grant, "right", "completed", success)["finalState"] == "SUCCEEDED"
    claim("left", "after-success", "empty")
    terminal = http(run_path, user)
    assert terminal["state"] == "SUCCEEDED" and len(terminal["tasks"]) == 3
    assert sorted(item["attempt"] for item in terminal["tasks"]) == [1, 1, 2]
    assert terminal["labels"]["arco.accepted_plan_sha256"] == plan_sha
    save(root / "terminal-run.json", terminal)
    return {"runId": first["runId"], "planId": first["planId"], "acceptedPlanSha256": plan_sha,
            "retryNotBefore": before["response"]["nextDeadline"], "beforeDeadlineWake": before,
            "scheduledRetryWake": wake, "apiRestart": {"oldPid": first_start["pid"], "newPid": restarted["pid"],
            "killedAt": killed["at"]}, "originalGeneration": left["generation"],
            "retryGeneration": grant["generation"], "permits": 2, "tasks": 3}


def s3_call(aws, env, bucket, hosted, operation, *options):
    connection = (["--endpoint-url", env["AWS_ENDPOINT"]] if not hosted else [])
    owner = ["--expected-bucket-owner", OWNER] if hosted else []
    result = subprocess.run(
        [str(aws), "--region", env["AWS_REGION"], "--no-cli-pager", *connection, "s3api",
         operation, "--bucket", bucket, *owner, *options],
        env=env, capture_output=True, text=True, timeout=15, check=False,
    )
    if result.returncode:
        raise RuntimeError(f"{operation} failed: {redact(result.stderr, env).strip()}")
    return json.loads(result.stdout)


def verify_s3(root, aws, env, bucket, prefix, hosted, scenario):
    def fetch(label, key):
        head = s3_call(aws, env, bucket, hosted, "head-object", "--key", key)
        version, tag = head["VersionId"], head["ETag"]
        assert version not in ("", "null") and 0 < head["ContentLength"] < 1024 * 1024
        path = root / f"{label}.json"
        meta = s3_call(aws, env, bucket, hosted, "get-object", "--key", key, "--if-match", tag, str(path))
        assert (meta["VersionId"], meta["ETag"]) == (version, tag)
        assert head["ContentLength"] == path.stat().st_size
        return read(path), {"Key": key, "VersionId": version, "ETag": tag, "sha256": sha256(path)}

    workspace = f"{prefix}/tenant={TENANT}/workspace={WORKSPACE}/"
    lock, lock_id = fetch("raw-lock", prefix + "/locks/orchestration.compaction.lock.json")
    assert lock.get("operation") is None and datetime.fromisoformat(lock["expiresAt"]) < datetime.now(timezone.utc)
    pointer, pointer_id = fetch("selected-pointer", workspace + "state/orchestration/manifest.pointer.json")
    manifest_path = pointer["manifest_path"]
    assert re.fullmatch(r"state/orchestration/manifests/[0-9]{20}\.json", manifest_path)
    selected, manifest_id = fetch("selected-manifest", workspace + manifest_path)
    assert selected["manifest_id"] == pointer["manifest_id"]
    control = selected["worker_pull"]
    accept = control["accepted_runs"][scenario["runId"]]
    assert accept["sha256"] == scenario["acceptedPlanSha256"]
    assert accept["plan_id"] == scenario["planId"] and accept["retry_policy"] == RETRY_POLICY
    assert len(control["accepted_runs"]) == 1 and control["permits"] == [None, None]
    receipts = control["receipts"]
    assert set(receipts) == {"first", "second", "backlog", "retry"}
    assert receipts["first"]["completion"]["state"] == "FAILED"
    assert all(receipts[key]["completion"]["state"] == "SUCCEEDED" for key in ("second", "backlog", "retry"))
    return {"scope": prefix + "/", "lock": lock_id, "pointer": pointer_id, "manifest": manifest_id}


def qualify(binary, root, aws, env, bucket, prefix, hosted):
    with socket.socket() as http, socket.socket() as grpc:
        http.bind(("127.0.0.1", 0))
        grpc.bind(("127.0.0.1", 0))
        http_port, grpc_port = http.getsockname()[1], grpc.getsockname()[1]
    origin = f"http://127.0.0.1:{http_port}"
    host = {"permits": 2, "callbackBaseUrl": origin + PULL, "claimKey": list(secrets.token_bytes(32)),
            "controlToken": secrets.token_hex(28), "sessions": {s: secrets.token_hex(28) for s in ("left", "right")}}
    config = root / "pilot.json"
    save(config, {"tenant": TENANT, "workspace": WORKSPACE, "storagePrefix": prefix, "host": host})
    config.chmod(0o600)
    user_secret = secrets.token_hex(32)
    environment = {**env, "ARCO_FLOW_WORKER_PULL_PILOT_PATH": str(config), "ARCO_BIND_IP": "127.0.0.1",
                   "ARCO_ENVIRONMENT": "staging", "ARCO_API_PUBLIC": "false", "ARCO_DEBUG": "false",
                   "ARCO_STORAGE_BUCKET": f"s3://{bucket}", "ARCO_HTTP_PORT": str(http_port),
                   "ARCO_GRPC_PORT": str(grpc_port), "ARCO_JWT_SECRET": user_secret,
                   "ARCO_JWT_ISSUER": "api-pilot", "ARCO_JWT_AUDIENCE": "arco-api",
                   "ARCO_TASK_TOKEN_SECRET": secrets.token_hex(32), "ARCO_TASK_TOKEN_ISSUER": "api-pilot",
                   "ARCO_TASK_TOKEN_AUDIENCE": "arco-worker", "PYTHONDONTWRITEBYTECODE": "1"}
    command = [sys.executable, "-B", str(Path(__file__).resolve()), "--scheduler",
               str(root), str(binary), origin]
    with (root / "scheduler.log").open("xb") as log:
        process = subprocess.Popen(command, env=environment, stdout=log, stderr=subprocess.STDOUT)
        try:
            scenario = exercise(root, process, origin, host, user_secret)
        finally:
            (root / "stop").touch()
            try:
                process.wait(timeout=25)
            except subprocess.TimeoutExpired:
                stop(process)
    assert process.returncode == 0, "Scheduler exited unsuccessfully"
    scheduler_calls = [e for e in records(root / "scheduler.jsonl") if e["event"] == "wake"]
    clients = records(root / "client.jsonl")
    remote_calls = scheduler_calls + [e for e in clients if e["path"].startswith(PULL)]
    ordinary_503s = [e for e in remote_calls if e["httpStatus"] == 503]
    assert not ordinary_503s, ordinary_503s
    maximum = max(e["wallMs"] for e in remote_calls)
    assert maximum <= 5000, f"Worker-pull HTTP wall time exceeded five seconds: {maximum} ms"
    physical = verify_s3(root, aws, env, bucket, prefix, hosted, scenario)
    return {**scenario, "schedulerPid": process.pid, "schedulerInvocations": len(scheduler_calls),
            "clientInvocations": len(clients), "maxWorkerPullWallMs": maximum,
            "ordinaryTimeouts": 0, "physicalIdentity": physical}


def main():
    if sys.flags.optimize:
        raise SystemExit("The proof requires assertions enabled")
    if len(sys.argv) == 5 and sys.argv[1] == "--scheduler":
        scheduler(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4])
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--run", required=True)
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--mode", choices=("minio", "hosted"), required=True)
    args = parser.parse_args()
    assert re.fullmatch(r"[a-z0-9][a-z0-9-]{0,70}", args.run)
    binary = args.binary.resolve(strict=True)
    assert sha256(binary) == args.expected_binary_sha256
    root = args.output.resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    aws = shutil.which("aws")
    assert aws, "AWS CLI is required"
    prefix = f"conformance/s3/flow-worker-pull-{args.run}/api-pilot"
    receipt = {"passed": False, "mode": args.mode, "scope": prefix + "/",
               "binarySha256": args.expected_binary_sha256, "harnessSha256": sha256(Path(__file__))}
    try:
        if args.mode == "hosted":
            document = sys.stdin.buffer.read(65_537)
            assert len(document) <= 65_536
            env = session_environment(json.loads(document), datetime.now(timezone.utc))
            assert env["AWS_REGION"] == REGION
            receipt.update(qualify(binary, root, aws, env, BUCKET, prefix, True))
        else:
            minio = shutil.which("minio")
            assert minio, "MinIO is required"
            with serve_minio(root, minio) as env:
                bucket, versioning = versioned_bucket(aws, env)
                assert versioning["Status"] == "Enabled"
                receipt.update(qualify(binary, root, aws, env, bucket, prefix, False))
                versions = aws_call(aws, env, "list-object-versions", "--bucket", bucket, "--no-paginate")
                save(root / "object-versions.json", versions)
                assert not versions.get("IsTruncated")
                entries = versions.get("Versions", []) + versions.get("DeleteMarkers", [])
                assert entries and all(entry["Key"].startswith(prefix + "/") for entry in entries)
                receipt["versioning"] = versioning
                receipt["versionRecords"] = len(entries)
        receipt["passed"] = True
    finally:
        save(root / "receipt.json", receipt)
        print(root / "receipt.json", flush=True)


if __name__ == "__main__":
    main()
