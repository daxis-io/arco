"""Run two worker-pull cases on the approved, existing Ohio S3 fixture."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from qualify_minio import Case, SCENARIOS, iso, pull, save, sha256

BUCKET = "arco-030-q-452456948477-20261024"
OWNER = "452456948477"
REGION = "us-east-2"
DEFAULT_CASES = ("race", "reply-loss")
CASE_LIMITS = {"race": 18, "reply-loss": 36, "rivers-retry": 32}
CASE_SECONDS = {"race": 60, "reply-loss": 120, "rivers-retry": 150}
SECONDS = 180
CLI_LIMIT = 12
OBJECT_LIMIT = 256


def session_environment(document, now):
    if not isinstance(document, dict):
        raise ValueError("expected a scoped STS credentials document")
    credentials = document.get("Credentials", document)
    if not isinstance(credentials, dict):
        raise ValueError("expected scoped STS credentials")
    key = credentials.get("AccessKeyId", "")
    if not isinstance(key, str) or not re.fullmatch(r"ASIA[A-Z0-9]{16}", key):
        raise ValueError("expected a temporary AWS access key")
    env = {
        "PATH": os.defpath, "AWS_REGION": REGION, "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_CONFIG_FILE": os.devnull, "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
        "AWS_MAX_ATTEMPTS": "1", "AWS_ACCESS_KEY_ID": key,
    }
    for field, variable, limit in [
        ("SecretAccessKey", "AWS_SECRET_ACCESS_KEY", 128),
        ("SessionToken", "AWS_SESSION_TOKEN", 8192),
    ]:
        value = credentials.get(field)
        if not isinstance(value, str) or not value or len(value) > limit or any(
            not 33 <= ord(ch) <= 126 for ch in value
        ):
            raise ValueError(f"missing or invalid scoped STS field {field}")
        env[variable] = value
    try:
        expires = datetime.fromisoformat(credentials["Expiration"])
        lifetime = (expires - now).total_seconds()
    except (KeyError, TypeError, ValueError):
        raise ValueError("expected a timezone-aware STS expiration") from None
    if lifetime < SECONDS + 30:
        raise ValueError("scoped STS session expires before the qualification window")
    return env


def evidence_root(run):
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?", run):
        raise ValueError("run ID must be a short lowercase slug")
    return f"conformance/s3/flow-worker-pull-{run}"


def redact(text, env):
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        text = text.replace(env[key], "[session credential]")
    return text


class HostedCase(Case):
    def __init__(self, root, binary, env, name, prefix, deadline):
        super().__init__(root, binary, None, env, name, BUCKET, f"{prefix}/{name}")
        self.env = dict(self.env, ARCO_PULL_TEST_MODE="hosted")
        self.deadline = deadline
        self.limit = CASE_LIMITS[name]
        self.active = {}
        self.invocations = 0
        self.logical_reads = 0
        self.logical_writes = 0

    def command_timeout(self):
        return max(0.001, min(CLI_LIMIT, self.deadline - time.monotonic()))

    def safe_text(self, text):
        return redact(text, self.env)

    def spawn(self, *args, **kwargs):
        if self.invocations >= self.limit or time.monotonic() >= self.deadline:
            raise RuntimeError("hosted fixture reached its process or time limit")
        self.invocations += 1
        child = super().spawn(*args, **kwargs)
        self.active[child[0].pid] = child[0]
        return child

    def finish(self, child, *args, **kwargs):
        result = super().finish(child, *args, **kwargs)
        self.active.pop(child[0].pid)
        if child[1] == "validate-s3":
            return result
        with self.log.open() as stream:
            record = json.loads(stream.readlines()[-1])
        count = json.loads(record["stdout"])["storageCalls"]
        assert 0 <= count["logicalReads"] <= 256 and 0 <= count["logicalWrites"] <= 64
        self.logical_reads += count["logicalReads"]
        self.logical_writes += count["logicalWrites"]
        return result

    def race(self, first, second):
        initial = super().race(first, second, expected_status=(200, 503), include_status=True)
        statuses = [reply["status"] for reply in initial]
        bodies = [reply["response"] for reply in initial]
        if statuses == [200, 200]:
            return bodies
        assert sorted(statuses) == [200, 503], initial
        loser = statuses.index(503)
        winner = 1 - loser
        assert isinstance(bodies[loser].get("error"), str), initial
        assert bodies[winner]["status"] == "granted", initial
        identities = [first, second]
        grant = bodies[winner]["grant"]
        prior_requests = ("original",) if self.root.name == "rivers-retry" else ()
        before = self.inspect("before-race-retry")
        check_race_winner(before, identities[winner], grant, prior_requests)
        # Retry exactly the same request ID on the same worker session.
        retry = self.claim(*identities[loser])
        assert retry["status"] == "busy", retry
        after = self.inspect("after-race-retry")
        check_race_winner(after, identities[winner], grant, prior_requests)
        assert before["pointerVersion"] == after["pointerVersion"]
        revision = before["manifest"]["revision_ulid"]
        assert revision == after["manifest"]["revision_ulid"]
        save(self.root / "race-retry.json", {
            "initialHttpStatuses": statuses,
            "winnerRequestId": identities[winner][1],
            "winnerGeneration": grant["generation"],
            "retriedSession": identities[loser][0],
            "retriedRequestId": identities[loser][1],
            "retryStatus": retry["status"],
            "selectedRevision": revision,
        })
        bodies[loser] = retry
        return bodies

    def close(self):
        for process in self.active.values():
            if process.poll() is None:
                process.kill()
            process.communicate()
        self.active.clear()

    def counts(self):
        return {
            "invocations": self.invocations,
            "logicalReads": self.logical_reads,
            "logicalWrites": self.logical_writes,
        }


def check_race_winner(state, identity, grant, prior_requests=()):
    session, request = identity
    authority = pull(state)
    assert grant["permit"] == 0 and authority["permits"] == [grant["generation"]]
    assert set(authority["receipts"]) == {request, *prior_requests}
    assert all(authority["receipts"][prior]["phase"] == "Completed" for prior in prior_requests)
    receipt = authority["receipts"][request]
    assert receipt["phase"] == "Active" and receipt["started_at"] is None
    assert receipt["session"] == session and receipt["generation"] == grant["generation"]
    assert receipt["permit"] == grant["permit"]
    assert receipt["deadline"] == grant["leaseExpiresAt"]
    assert receipt["start_before"] == grant["startBefore"] and time.time() < iso(receipt["deadline"])
    dispatch, envelope = receipt["dispatch"], grant["envelope"]
    for stored, wire in [
        ("dispatch_id", "dispatchId"), ("attempt_id", "attemptId"), ("attempt", "attempt"),
        ("run_id", "runId"), ("task_key", "taskKey"),
    ]:
        assert dispatch[stored] == envelope[wire]


class Inventory:
    def __init__(self, aws, env, deadline):
        self.aws, self.env, self.deadline = aws, env, deadline
        self.calls = 0

    def call(self, operation, *options):
        if self.calls >= 8 or time.monotonic() >= self.deadline:
            raise RuntimeError("hosted fixture reached its inventory limit")
        self.calls += 1
        result = subprocess.run(
            [self.aws, "--region", REGION, "--no-cli-pager", "--no-cli-auto-prompt",
             "s3api", operation, "--bucket", BUCKET, "--expected-bucket-owner", OWNER,
             *options],
            env=self.env, capture_output=True, text=True, check=False,
            timeout=max(0.001, min(CLI_LIMIT, self.deadline - time.monotonic())),
        )
        if result.returncode != 0:
            raise RuntimeError(f"{operation} failed: {redact(result.stderr, self.env).strip()}")
        return json.loads(result.stdout)

    def scoped_objects(self, case, label, limit=OBJECT_LIMIT):
        prefix = case.prefix + "/"
        listing = self.call("list-objects-v2", "--prefix", prefix,
                            "--max-keys", str(limit), "--no-paginate")
        save(case.root / f"objects-{label}.json", listing)
        if listing.get("IsTruncated"):
            raise RuntimeError("hosted current-object inventory is incomplete")
        entries = listing.get("Contents", [])
        assert len(entries) <= limit and all(entry["Key"].startswith(prefix) for entry in entries)
        return entries

    def verify(self, case):
        entries = self.scoped_objects(case, "selected")
        state = case.inspect("versioned-metadata")
        lock_key = case.prefix + "/locks/orchestration.compaction.lock.json"
        pointer_key = case.prefix + "/tenant=proof/workspace=wake/state/orchestration/manifest.pointer.json"
        evidence = {"prefix": case.prefix, "currentObjects": len(entries)}
        for kind, key in [("lock", lock_key), ("pointer", pointer_key)]:
            listed, = [entry for entry in entries if entry["Key"] == key]
            head = self.call("head-object", "--key", key)
            version_id, etag = head["VersionId"], head["ETag"]
            assert version_id and version_id != "null" and etag == listed["ETag"]
            assert json.loads(state[f"{kind}Version"]) == {"version": version_id, "e_tag": etag}
            assert state[f"{kind}Etag"] == etag
            evidence[kind] = {"Key": key, "VersionId": version_id, "ETag": etag}
        save(case.root / "physical-identity.json", evidence)
        return evidence


def run(args, env):
    prefix = evidence_root(args.run)
    root = args.output.absolute()
    binary = args.binary.resolve(strict=True)
    binary_sha = sha256(binary)
    if binary_sha != args.expected_binary_sha256:
        raise ValueError("copied executable hash mismatch")
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    deadline = time.monotonic() + SECONDS
    cases = {}
    receipt = {
        "bucket": BUCKET, "owner": OWNER, "region": REGION, "scope": prefix,
        "binarySha256": binary_sha, "harnessSha256": sha256(Path(__file__)),
        "sharedScenariosSha256": sha256(Path(__file__).with_name("qualify_minio.py")),
        "validateOnly": args.validate_only, "secondsLimit": SECONDS, "cases": {},
        "historyVerification": "external owner evidence", "secondsPerCase": CASE_SECONDS,
    }
    inventory = None
    try:
        names = args.case or DEFAULT_CASES
        cases = {name: HostedCase(root, binary, env, name, prefix, deadline) for name in names}
        if args.validate_only:
            for name, case in cases.items():
                # The example validates without constructing a storage adapter.
                save(case.config, {"tenant": "proof", "workspace": "wake", "host": {
                    "permits": 1, "callbackBaseUrl": "http://127.0.0.1:8297",
                    "claimKey": [33] * 32, "controlToken": "local-proof-token-for-guard-only-000",
                    "sessions": {"worker": "local-proof-worker-for-guard-only-00"},
                }})
                value = case.invoke("validate-s3")
                assert value["hosted"] and value["physicalPrefix"] == case.prefix + "/"
                receipt["cases"][name] = value
        else:
            aws = shutil.which("aws")
            if not aws:
                raise RuntimeError("AWS CLI is required for scoped inventory")
            inventory = Inventory(str(Path(aws).resolve(strict=True)), env, deadline)
            for name, case in cases.items():
                case.deadline = min(deadline, time.monotonic() + CASE_SECONDS[name])
                inventory.deadline = case.deadline
                phase = "prefix"
                try:
                    if inventory.scoped_objects(case, "before", limit=1):
                        raise RuntimeError("hosted qualification prefix already contains objects")
                    phase = "scenario"
                    verdict = SCENARIOS[name](case)
                    phase = "inventory"
                    physical = inventory.verify(case)
                    receipt["cases"][name] = {"passed": True, **verdict, "physicalIdentity": physical}
                except Exception as error:
                    receipt["cases"][name] = {
                        "passed": False, "failedPhase": phase,
                        "error": redact(str(error) or error.__class__.__name__, env),
                    }
                finally:
                    case.close()
        receipt["passed"] = all(case.get("passed", args.validate_only) for case in receipt["cases"].values())
    except Exception as error:
        receipt["passed"] = False
        receipt["error"] = redact(str(error) or error.__class__.__name__, env)
    finally:
        receipt["processes"] = {name: case.counts() for name, case in cases.items()}
        receipt["inventoryCalls"] = inventory.calls if inventory else 0
        for case in cases.values():
            case.close()
        save(root / "receipt.json", receipt)
    print(root / "receipt.json")
    return 0 if receipt["passed"] else 1


def main():
    if sys.flags.optimize:
        raise SystemExit("qualification requires assertions enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--run", required=True)
    parser.add_argument("--expected-binary-sha256", required=True)
    parser.add_argument("--case", choices=CASE_LIMITS, action="append")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    if args.case and len(set(args.case)) != len(args.case):
        parser.error("repeat each requested case at most once")
    if args.case and len(args.case) > 2:
        parser.error("run at most two cases per scoped inventory")
    try:
        data = sys.stdin.buffer.read(65_537)
        if len(data) > 65_536:
            raise ValueError("scoped STS document exceeds its size limit")
        try:
            session = json.loads(data)
        except (UnicodeDecodeError, ValueError):
            raise ValueError("invalid scoped STS credentials document") from None
        env = session_environment(session, datetime.now(timezone.utc))
        return run(args, env)
    except (TypeError, ValueError, OSError) as error:
        raise SystemExit(str(error)) from None


if __name__ == "__main__":
    raise SystemExit(main())
