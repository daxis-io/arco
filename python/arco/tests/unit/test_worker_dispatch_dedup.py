"""Tests for dispatch_id deduplication in the worker (issue #328)."""

from __future__ import annotations

import threading
from typing import Any

import pytest

from arco_flow.cli.config import ArcoFlowConfig
from arco_flow.client import ApiError
from arco_flow.context import AssetContext
from arco_flow.types import AssetOut
from arco_flow.worker.server import DispatchWorker, WorkerDispatchEnvelope


def _sample_envelope_dict() -> dict[str, Any]:
    return {
        "tenant_id": "tenant-a",
        "workspace_id": "workspace-b",
        "run_id": "run-123",
        "task_id": "ct1_run-123_daily-sales",
        "task_key": "analytics.daily_sales",
        "attempt": 1,
        "attempt_id": "att-1",
        "dispatch_id": "dispatch:run-123:analytics.daily_sales:1",
        "worker_queue": "default-queue",
        "callback_base_url": "https://callbacks.example",
        "task_token": "token-from-envelope",
        "token_expires_at": "2026-01-01T00:00:00Z",
        "traceparent": None,
        "payload": {},
    }


class _RecordingClient:
    def __init__(self) -> None:
        self.started_calls: list[dict[str, Any]] = []
        self.completed_calls: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    def task_started(self, **kwargs: Any) -> None:
        with self.lock:
            self.started_calls.append(kwargs)

    def task_completed(self, **kwargs: Any) -> None:
        with self.lock:
            self.completed_calls.append(kwargs)

    def task_heartbeat(self, **kwargs: Any) -> None:
        _ = kwargs

    def upload_logs(self, **kwargs: Any) -> None:
        _ = kwargs

    def close(self) -> None:
        return


def _make_worker(asset_fn: Any, client: Any) -> DispatchWorker:
    worker = object.__new__(DispatchWorker)
    worker.config = ArcoFlowConfig(
        debug=True,
        api_url="https://callbacks.example",
        tenant_id="tenant-a",
        workspace_id="workspace-b",
    )
    worker.worker_id = "worker-1"
    worker._fallback_task_token = "fallback-token"
    worker._client = client
    worker._assets = {"analytics.daily_sales": asset_fn}
    worker._partitioned_assets = set()
    worker._init_dispatch_state()
    return worker


def test_same_dispatch_id_delivered_twice_executes_once() -> None:
    executions: list[str] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        executions.append(ctx.run_id)
        return AssetOut([], row_count=1)

    client = _RecordingClient()
    worker = _make_worker(asset_fn, client)
    envelope = WorkerDispatchEnvelope.from_dict(_sample_envelope_dict())

    worker.handle_dispatch(envelope)
    worker.handle_dispatch(envelope)

    assert len(executions) == 1
    assert len(client.started_calls) == 1
    assert len(client.completed_calls) == 1


def test_concurrent_duplicate_delivery_does_not_spawn_second_execution() -> None:
    first_started = threading.Event()
    release_first = threading.Event()
    executions: list[str] = []
    execution_lock = threading.Lock()

    def asset_fn(ctx: AssetContext) -> AssetOut:
        with execution_lock:
            executions.append(ctx.run_id)
        first_started.set()
        assert release_first.wait(timeout=10), "test deadlock: release never signalled"
        return AssetOut([], row_count=1)

    client = _RecordingClient()
    worker = _make_worker(asset_fn, client)
    envelope = WorkerDispatchEnvelope.from_dict(_sample_envelope_dict())

    first = threading.Thread(target=worker.handle_dispatch, args=(envelope,))
    first.start()
    assert first_started.wait(timeout=10), "first execution never started"

    # Redelivery arrives while the first execution is still running.
    worker.handle_dispatch(envelope)

    release_first.set()
    first.join(timeout=10)
    assert not first.is_alive()

    assert len(executions) == 1
    assert len(client.started_calls) == 1
    assert len(client.completed_calls) == 1


def test_distinct_dispatch_ids_both_execute() -> None:
    executions: list[str] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        executions.append(ctx.run_id)
        return AssetOut([], row_count=1)

    client = _RecordingClient()
    worker = _make_worker(asset_fn, client)

    first = _sample_envelope_dict()
    second = _sample_envelope_dict()
    second["attempt"] = 2
    second["attempt_id"] = "att-2"
    second["dispatch_id"] = "dispatch:run-123:analytics.daily_sales:2"

    worker.handle_dispatch(WorkerDispatchEnvelope.from_dict(first))
    worker.handle_dispatch(WorkerDispatchEnvelope.from_dict(second))

    assert len(executions) == 2
    assert len(client.completed_calls) == 2


def test_recent_dispatch_registry_is_bounded() -> None:
    def asset_fn(_ctx: AssetContext) -> AssetOut:
        return AssetOut([], row_count=1)

    client = _RecordingClient()
    worker = _make_worker(asset_fn, client)

    from arco_flow.worker.server import RECENT_DISPATCH_LIMIT

    for index in range(RECENT_DISPATCH_LIMIT + 10):
        worker._record_dispatch_completed(f"dispatch:run-123:analytics.daily_sales:{index}")

    assert len(worker._recent_dispatch_ids) == RECENT_DISPATCH_LIMIT


class _FailFirstStartedClient(_RecordingClient):
    """Recording client whose first task_started call fails transiently."""

    def __init__(self) -> None:
        super().__init__()
        self._started_failures_remaining = 1

    def task_started(self, **kwargs: Any) -> None:
        with self.lock:
            self.started_calls.append(kwargs)
            if self._started_failures_remaining > 0:
                self._started_failures_remaining -= 1
                raise ApiError(500, "transient control-plane error")


def test_failed_task_started_does_not_poison_dedup_and_redelivery_executes() -> None:
    """A dispatch that raised before its terminal report must re-execute on redelivery.

    Reproduces the reviewer's probe: the first delivery dies in the
    `task_started` callback (transient ApiError -> HTTP 500 -> Cloud Tasks
    redelivers the same deterministic dispatch_id). If that failure were
    recorded as "recently completed", the redelivery would be duplicate-acked
    and the task would be stuck in Dispatched forever, unrescuable by the
    sweeper (which reuses the same dispatch_id).
    """
    executions: list[str] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        executions.append(ctx.run_id)
        return AssetOut([], row_count=1)

    client = _FailFirstStartedClient()
    worker = _make_worker(asset_fn, client)
    envelope = WorkerDispatchEnvelope.from_dict(_sample_envelope_dict())

    with pytest.raises(ApiError):
        worker.handle_dispatch(envelope)

    # Nothing executed and no terminal report was sent, so the dispatch_id
    # must not be remembered as completed (and must not stay in flight).
    assert executions == []
    assert len(client.completed_calls) == 0
    assert envelope.dispatch_id not in worker._recent_dispatch_ids
    assert envelope.dispatch_id not in worker._inflight_dispatch_ids

    # Cloud Tasks redelivers after the 500: the task must actually execute.
    worker.handle_dispatch(envelope)

    assert len(executions) == 1
    assert len(client.started_calls) == 2
    assert len(client.completed_calls) == 1

    # Now that the terminal report was delivered, a further redelivery is
    # duplicate-acked without re-execution.
    worker.handle_dispatch(envelope)
    assert len(executions) == 1
    assert len(client.completed_calls) == 1
