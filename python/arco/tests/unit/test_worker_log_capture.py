"""Concurrency and authentication contracts for worker log upload."""

from __future__ import annotations

import sys
import threading
from typing import Any

from arco_flow.cli.config import ArcoFlowConfig
from arco_flow.context import AssetContext
from arco_flow.worker.server import DispatchWorker, WorkerDispatchEnvelope


class _RecordingClient:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.log_calls: list[dict[str, Any]] = []

    def task_started(self, **kwargs: Any) -> None:
        _ = kwargs

    def task_completed(self, **kwargs: Any) -> None:
        _ = kwargs

    def upload_logs(self, **kwargs: Any) -> None:
        with self._lock:
            self.log_calls.append(kwargs)


def _envelope(task_key: str) -> WorkerDispatchEnvelope:
    return WorkerDispatchEnvelope.from_dict(
        {
            "tenantId": "tenant-a",
            "workspaceId": "workspace-b",
            "runId": f"run-{task_key}",
            "taskId": f"task-{task_key}",
            "taskKey": task_key,
            "attempt": 1,
            "attemptId": f"attempt-{task_key}",
            "dispatchId": f"dispatch-{task_key}",
            "workerQueue": "default",
            "callbackBaseUrl": "https://callbacks.example",
            "taskToken": f"token-{task_key}",
            "tokenExpiresAt": "2026-08-03T00:00:00Z",
            "traceparent": None,
            "payload": {},
        }
    )


def _worker(
    client: _RecordingClient,
    assets: dict[str, Any],
) -> DispatchWorker:
    worker = object.__new__(DispatchWorker)
    worker.config = ArcoFlowConfig(
        debug=True,
        api_url="https://callbacks.example",
        tenant_id="tenant-a",
        workspace_id="workspace-b",
    )
    worker.worker_id = "worker-1"
    worker._fallback_task_token = worker.worker_id
    worker._client = client
    worker._assets = assets
    return worker


def test_concurrent_dispatches_upload_only_task_local_output() -> None:
    task_a_entered = threading.Event()
    task_b_entered = threading.Event()
    task_a_printed_while_b_active = threading.Event()

    def asset_a(_ctx: AssetContext) -> None:
        print("only-task-a-before")
        task_a_entered.set()
        assert task_b_entered.wait(timeout=5)
        print("only-task-a-after")
        task_a_printed_while_b_active.set()

    def asset_b(_ctx: AssetContext) -> None:
        assert task_a_entered.wait(timeout=5)
        print("only-task-b-before")
        task_b_entered.set()
        assert task_a_printed_while_b_active.wait(timeout=5)
        print("only-task-b-after")

    client = _RecordingClient()
    worker = _worker(client, {"task-a": asset_a, "task-b": asset_b})

    errors: list[Exception] = []

    def dispatch(task_key: str) -> None:
        try:
            worker.handle_dispatch(_envelope(task_key))
        except Exception as error:
            errors.append(error)

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    threads = [
        threading.Thread(target=dispatch, args=("task-a",)),
        threading.Thread(target=dispatch, args=("task-b",)),
    ]
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
    finally:
        # The baseline implementation can leave process-global redirects installed when
        # concurrent contexts unwind out of order. Keep the red test isolated from pytest.
        sys.stdout = original_stdout
        sys.stderr = original_stderr

    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    logs_by_task = {call["task_key"]: call for call in client.log_calls}
    assert set(logs_by_task) == {"task-a", "task-b"}
    assert "only-task-a-before" in logs_by_task["task-a"]["stdout"]
    assert "only-task-a-after" in logs_by_task["task-a"]["stdout"]
    assert "only-task-b" not in logs_by_task["task-a"]["stdout"]
    assert "only-task-b-before" in logs_by_task["task-b"]["stdout"]
    assert "only-task-b-after" in logs_by_task["task-b"]["stdout"]
    assert "only-task-a" not in logs_by_task["task-b"]["stdout"]
    assert logs_by_task["task-a"]["task_token"] == _envelope("task-a").task_token
    assert logs_by_task["task-b"]["task_token"] == _envelope("task-b").task_token
    assert logs_by_task["task-a"]["callback_base_url"] == "https://callbacks.example"
    assert logs_by_task["task-b"]["callback_base_url"] == "https://callbacks.example"
