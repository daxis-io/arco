"""Exercise the hosted race oracle with simulated public-route responses."""

import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import time
from unittest.mock import Mock, patch

sys.dont_write_bytecode = True
from qualify_hosted_s3 import HostedCase, evidence_root, session_environment, sha256
from qualify_minio import save


def child(pid, status, response):
    process = Mock()
    process.pid = pid
    process.returncode = 0
    process.poll.return_value = None
    process.communicate.return_value = (json.dumps({
        "status": status, "response": response,
        "storageCalls": {"logicalReads": 2, "logicalWrites": 2},
    }).encode(), b"")
    return process


def run(binary, root):
    instant = datetime.now(timezone.utc)
    env = session_environment({
        "AccessKeyId": "ASIA0000000000000000", "SecretAccessKey": "synthetic-oracle-secret",
        "SessionToken": "synthetic-oracle-token",
        "Expiration": (instant + timedelta(minutes=10)).isoformat(),
    }, instant)
    lease = (instant + timedelta(seconds=30)).isoformat()
    winner = {
        "status": "granted", "grant": {
            "generation": 4, "permit": 0, "leaseExpiresAt": lease, "startBefore": lease,
            "envelope": {"dispatchId": "dispatch", "attemptId": "attempt", "attempt": 1,
                         "runId": "run", "taskKey": "task"},
        },
    }
    receipt = {
        "session": "worker", "generation": 4, "permit": 0, "phase": "Active",
        "deadline": lease, "start_before": lease, "started_at": None,
        "dispatch": {"dispatch_id": "dispatch", "attempt_id": "attempt", "attempt": 1,
                     "run_id": "run", "task_key": "task"},
    }
    selected = {"pointerVersion": "winner-version", "manifest": {
        "revision_ulid": "winner-revision", "worker_pull": {"permits": [4], "receipts": {"first": receipt}},
    }}
    busy = {"status": "busy"}
    unavailable = {"error": "worker-pull authority needs retry"}
    identities = [("worker", "first"), ("other", "second")]
    checked = []

    def fixture(label, first, second, views, retry, expected_pass, expected_retries, case_name="race"):
        directory = root / label
        directory.mkdir()
        case = HostedCase(directory, binary, env, case_name, evidence_root("oracle"), time.monotonic() + 10)
        with (
            patch("qualify_minio.subprocess.Popen", side_effect=[child(17, *first), child(23, *second)]),
            patch.object(case, "inspect", side_effect=views),
            patch.object(case, "claim", return_value=retry) as replay,
        ):
            try:
                outcome = case.race(*identities)
            except AssertionError:
                if expected_pass:
                    raise
            else:
                assert expected_pass and outcome == [winner, busy]
            assert replay.call_count == expected_retries
            if expected_retries:
                replay.assert_called_once_with("other", "second")
                if expected_pass:
                    proof = json.loads((case.root / "race-retry.json").read_text())
                    assert proof["initialHttpStatuses"] == [200, 503]
                    assert proof["winnerGeneration"] == 4 and proof["retriedRequestId"] == "second"
        checked.append(label)

    fixture("same-request-busy", (200, winner), (503, unavailable), [selected, selected], busy, True, 1)
    fixture("normal-two-responses", (200, winner), (200, busy), [], busy, True, 0)
    fixture("missing-grant", (200, busy), (503, unavailable), [], busy, False, 0)
    fixture("unexpected-status", (200, winner), (409, unavailable), [], busy, False, 0)
    fixture("retry-grants", (200, winner), (503, unavailable), [selected], winner, False, 1)
    changed = deepcopy(selected)
    changed["pointerVersion"] = "other-version"
    fixture("selected-pointer-moved", (200, winner), (503, unavailable), [selected, changed], busy, False, 1)
    extra = deepcopy(selected)
    extra["manifest"]["worker_pull"]["permits"].append(5)
    fixture("extra-permit", (200, winner), (503, unavailable), [extra], busy, False, 0)
    expired = deepcopy(selected)
    expired["manifest"]["worker_pull"]["receipts"]["first"]["deadline"] = instant.isoformat()
    fixture("expired-winner", (200, winner), (503, unavailable), [expired], busy, False, 0)
    prior = deepcopy(selected)
    prior["manifest"]["worker_pull"]["receipts"]["original"] = {"phase": "Completed"}
    fixture("rivers-completed-prior", (200, winner), (503, unavailable), [prior, prior], busy, True, 1, "rivers-retry")
    fixture("rivers-missing-prior", (200, winner), (503, unavailable), [selected], busy, False, 0, "rivers-retry")
    active = deepcopy(prior)
    active["manifest"]["worker_pull"]["receipts"]["original"]["phase"] = "Active"
    fixture("rivers-active-prior", (200, winner), (503, unavailable), [active], busy, False, 0, "rivers-retry")
    save(root / "receipt.json", {"passed": True, "binarySha256": sha256(binary), "scenarios": checked})
    print(f"{len(checked)} hosted race oracle checks passed with simulated HTTP responses.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    run(args.binary.resolve(strict=True), args.output)
