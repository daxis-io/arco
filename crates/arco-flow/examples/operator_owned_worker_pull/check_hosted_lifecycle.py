"""Check start order and isolated case outcomes with local fixture doubles."""

import argparse
from copy import deepcopy
import json
from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.dont_write_bytecode = True
from qualify_minio import competing_claims, reply_loss, save, sha256
from qualify_hosted_s3 import run


class LifecycleCase:
    def __init__(self, root):
        self.root = root
        self.root.mkdir()
        self.next_start = None
        self.grants = {
            4: self.grant(4, 0), 6: self.grant(6, 1),
        }
        self.deadline = {}
        self.finished = set()
        self.stage = "empty"
        self.replacement = False
        self.starts = []

    @staticmethod
    def grant(generation, slot):
        return {
            "generation": generation, "permit": 0,
            "startBefore": "2026-11-25T10:00:15Z",
            "leaseExpiresAt": "2026-11-25T10:00:15Z",
            "envelope": {"taskKey": f"task-{slot:03}", "taskId": f"ct1_{slot}",
                         "attempt": 1, "attemptId": f"attempt-{slot}"},
        }

    def guard(self, action):
        assert self.next_start is None, f"{action} preceded start of grant {self.next_start}"

    def seed(self, task_count=1):
        self.replacement = task_count == 2
        self.stage = "seeded"
        return {}

    def race(self, *_identities):
        self.next_start = 4
        self.stage = "claimed"
        return [{"status": "granted", "grant": deepcopy(self.grants[4])}, {"status": "busy"}]

    def claim(self, worker, request, expected_status=200, fault=None):
        self.guard("claim")
        if fault:
            assert self.replacement and request == "original"
            self.stage = "claimed"
            save(self.root / "lost-grant.json", {})
            return {"error": "lost response"}
        original = "original" if self.replacement else "first"
        if request == original and worker == "worker":
            if 4 in self.finished:
                return {"status": "completed", "finalState": "SUCCEEDED", "acceptedAt": "completed-time"}
            grant = deepcopy(self.grants[4])
            if 4 not in self.deadline:
                self.next_start = 4
            else:
                grant["leaseExpiresAt"] = self.deadline[4]
            return {"status": "granted", "grant": grant}
        if self.replacement and request == "replacement" and 4 in self.finished:
            self.next_start = 6
            return {"status": "granted", "grant": deepcopy(self.grants[6])}
        if expected_status == 409:
            return {"error": "conflicting session"}
        return {"status": "busy"}

    def fact(self, _case, grant, worker, kind, expected_status=200, fault=None, **_fields):
        generation = grant["generation"]
        if self.next_start is not None:
            assert kind == "started" and generation == self.next_start
            self.next_start = None
            self.starts.append((generation, worker))
        if fault:
            save(self.root / f"lost-{kind}.json", {})
        if expected_status == 409:
            return {"error": "stale callback"}
        if kind == "started":
            self.deadline.setdefault(generation, "2026-11-25T10:01:31Z")
            self.stage = "started"
            return {"acknowledged": True, "serverTime": "started-time"}
        if kind == "heartbeat":
            self.deadline[generation] = "2026-11-25T10:01:40Z"
            self.stage = "heartbeat"
            return {"acknowledged": True}
        self.finished.add(generation)
        self.stage = "completed"
        return {"acknowledged": True, "finalState": "SUCCEEDED", "serverTime": "completed-time"}

    def inspect(self, label):
        self.guard(f"inspect {label}")
        target = "original" if self.replacement else "first"
        running = 4 in self.deadline
        live = 6 if 6 in self.deadline else 4
        permits = [None if live in self.finished else live]
        receipt = {
            "generation": 4, "permit": 0, "session": "worker",
            "started_at": "started-time" if running else None,
            "deadline": self.deadline.get(4, self.grants[4]["leaseExpiresAt"]),
        }
        return {
            "manifest": {"worker_pull": {"permits": permits, "receipts": {target: receipt}}},
            "tasks": [{"state": "RUNNING", "attempt": 1, "attempt_id": "attempt-0"}],
            "dispatches": [{"id": "other-task"}],
            "runs": {"wake-proof": {"state": "SUCCEEDED", "tasks_completed": len(self.finished),
                                  "tasks_succeeded": len(self.finished)}},
        }


def check_order(root):
    race = LifecycleCase(root / "race-order")
    with patch("qualify_minio.fact", side_effect=race.fact):
        assert competing_claims(race) == {"winner": "first", "generation": 4}
    assert race.starts == [(4, "worker")]

    losses = LifecycleCase(root / "reply-loss-order")
    with (
        patch("qualify_minio.fact", side_effect=losses.fact),
        patch("qualify_minio.check_cas"),
    ):
        assert reply_loss(losses) == {"generation": 4, "acceptedCompletionTime": "completed-time"}
    assert losses.starts == [(4, "worker"), (6, "other")]


def check_case_isolation(binary, root):
    for failed_case in ("race", "reply-loss"):
        results = []

        def exercise(name):
            def execute(_case):
                results.append(name)
                if name == failed_case:
                    raise RuntimeError("fixture scenario failure")
                return {"result": "verified"}
            return execute

        supervisor = SimpleNamespace(
            run="isolated", output=root / f"failure-{failed_case}", binary=binary,
            expected_binary_sha256=sha256(binary), validate_only=False, case=None,
        )
        inventory = Mock()
        inventory.calls = 0
        inventory.scoped_objects.return_value = []
        inventory.verify.return_value = {"currentObjects": 2}
        fixtures = {
            name: Mock(counts=Mock(return_value={}), deadline=0) for name in ("race", "reply-loss")
        }
        with (
            patch("qualify_hosted_s3.HostedCase", side_effect=list(fixtures.values())),
            patch("qualify_hosted_s3.Inventory", return_value=inventory),
            patch("qualify_hosted_s3.shutil.which", return_value=str(binary)),
            patch("qualify_hosted_s3.SCENARIOS", {name: exercise(name) for name in fixtures}),
        ):
            assert run(supervisor, {
                "AWS_ACCESS_KEY_ID": "dummy-key", "AWS_SECRET_ACCESS_KEY": "dummy-secret",
                "AWS_SESSION_TOKEN": "dummy-token",
            }) == 1
        assert results == ["race", "reply-loss"]
        proof = json.loads((supervisor.output / "receipt.json").read_text())
        assert proof["cases"][failed_case]["passed"] is False
        successful_case = next(name for name in fixtures if name != failed_case)
        assert proof["cases"][successful_case]["passed"] is True
        assert all(fixture.close.called for fixture in fixtures.values())


if __name__ == "__main__":
    if sys.flags.optimize:
        raise SystemExit("qualification requires assertions enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=False)
    check_order(args.output)
    check_case_isolation(binary, args.output)
    save(args.output / "receipt.json", {
        "passed": True, "binarySha256": sha256(binary),
        "checks": ["race starts winner first", "reply-loss starts both grants promptly",
                   "race failure leaves reply-loss runnable", "reply-loss failure preserves race success"],
    })
    print("4 hosted lifecycle and isolation checks passed.")
