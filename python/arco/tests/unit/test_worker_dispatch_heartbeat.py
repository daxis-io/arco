"""Worker heartbeat lifecycle regressions."""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any

from arco_flow.cli.config import ArcoFlowConfig
from arco_flow.client import ApiError, ApiResponse
from arco_flow.context import AssetContext
from arco_flow.types import AssetOut
from arco_flow.worker.server import (
    DEFAULT_HEARTBEAT_TIMEOUT_SEC,
    HEARTBEAT_INTERVAL_DIVISOR,
    DispatchWorker,
    WorkerDispatchEnvelope,
    _heartbeat_interval_sec,
)


def _envelope_dict(*, heartbeat_timeout_sec: int | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if heartbeat_timeout_sec is not None:
        payload["heartbeatTimeoutSec"] = heartbeat_timeout_sec
    return {
        "tenantId": "tenant-a",
        "workspaceId": "workspace-b",
        "runId": "run-123",
        "taskId": "ct1_run-123_daily-sales",
        "taskKey": "analytics.daily_sales",
        "attempt": 1,
        "attemptId": "att-1",
        "dispatchId": "dispatch:run-123:analytics.daily_sales:1",
        "workerQueue": "default-queue",
        "callbackBaseUrl": "https://callbacks.example",
        "taskToken": "token-from-envelope",
        "tokenExpiresAt": "2026-01-01T00:00:00Z",
        "traceparent": None,
        "payload": payload,
    }


class _RecordingClient:
    def __init__(self, *, heartbeat_response: dict[str, Any] | None = None) -> None:
        self.calls: list[str] = []
        self.heartbeat_calls: list[dict[str, Any]] = []
        self.completed_calls: list[dict[str, Any]] = []
        self.heartbeat_seen = threading.Event()
        self.heartbeat_response = heartbeat_response or {
            "acknowledged": True,
            "shouldCancel": False,
        }

    def task_started(self, **kwargs: Any) -> None:
        _ = kwargs
        self.calls.append("started")

    def task_heartbeat(self, **kwargs: Any) -> ApiResponse:
        self.calls.append("heartbeat")
        self.heartbeat_calls.append(kwargs)
        self.heartbeat_seen.set()
        return ApiResponse(payload=self.heartbeat_response)

    def task_completed(self, **kwargs: Any) -> None:
        self.calls.append("completed")
        self.completed_calls.append(kwargs)

    def upload_logs(self, **kwargs: Any) -> None:
        _ = kwargs
        self.calls.append("logs")

    def close(self) -> None:
        return


def _bare_worker(*, client: Any, assets: dict[str, Any]) -> DispatchWorker:
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
    worker._assets = assets
    worker._dispatch_lock = threading.Lock()
    worker._inflight_dispatches = set()
    worker._recent_dispatches = OrderedDict()
    return worker


def test_dispatch_worker_heartbeats_while_the_asset_runs() -> None:
    client = _RecordingClient()

    def asset_fn(_ctx: AssetContext) -> AssetOut:
        assert client.heartbeat_seen.wait(timeout=5)
        return AssetOut([], row_count=1)

    worker = _bare_worker(client=client, assets={"analytics.daily_sales": asset_fn})
    worker.handle_dispatch(
        WorkerDispatchEnvelope.from_dict(_envelope_dict(heartbeat_timeout_sec=3))
    )

    assert len(client.heartbeat_calls) == 1
    heartbeat = client.heartbeat_calls[0]
    assert heartbeat["task_id"] == "ct1_run-123_daily-sales"
    assert heartbeat["attempt_id"] == "att-1"
    assert client.calls.index("heartbeat") < client.calls.index("completed")
    assert "heartbeat" not in client.calls[client.calls.index("completed") :]


def test_dispatch_worker_reports_cancellation_requested_by_heartbeat() -> None:
    client = _RecordingClient(
        heartbeat_response={
            "acknowledged": True,
            "shouldCancel": True,
            "cancelReason": "run cancelled",
        }
    )

    def asset_fn(_ctx: AssetContext) -> AssetOut:
        assert client.heartbeat_seen.wait(timeout=5)
        return AssetOut([], row_count=1)

    worker = _bare_worker(client=client, assets={"analytics.daily_sales": asset_fn})
    worker.handle_dispatch(
        WorkerDispatchEnvelope.from_dict(_envelope_dict(heartbeat_timeout_sec=3))
    )

    completion = client.completed_calls[0]
    assert completion["outcome"] == "CANCELLED"
    assert completion["output"] is None
    assert completion["error"]["message"] == "run cancelled"


def test_dispatch_worker_survives_transient_heartbeat_failure() -> None:
    class FlakyClient(_RecordingClient):
        def task_heartbeat(self, **kwargs: Any) -> ApiResponse:
            super().task_heartbeat(**kwargs)
            raise ApiError(503, "callback transport failed")

    client = FlakyClient()

    def asset_fn(_ctx: AssetContext) -> AssetOut:
        assert client.heartbeat_seen.wait(timeout=5)
        return AssetOut([], row_count=1)

    worker = _bare_worker(client=client, assets={"analytics.daily_sales": asset_fn})
    worker.handle_dispatch(
        WorkerDispatchEnvelope.from_dict(_envelope_dict(heartbeat_timeout_sec=3))
    )

    assert client.completed_calls[0]["outcome"] == "SUCCEEDED"


def test_dispatch_worker_absorbs_superseded_completion() -> None:
    class SupersededClient(_RecordingClient):
        def task_completed(self, **kwargs: Any) -> None:
            super().task_completed(**kwargs)
            raise ApiError(409, "attempt_id_mismatch")

    executions: list[str] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        executions.append(ctx.run_id)
        return AssetOut([], row_count=1)

    worker = _bare_worker(
        client=SupersededClient(),
        assets={"analytics.daily_sales": asset_fn},
    )
    envelope = WorkerDispatchEnvelope.from_dict(_envelope_dict())

    worker.handle_dispatch(envelope)
    worker.handle_dispatch(envelope)

    assert executions == ["run-123"]


def test_dispatch_worker_propagates_genuine_completion_failure() -> None:
    class BrokenClient(_RecordingClient):
        def task_completed(self, **kwargs: Any) -> None:
            super().task_completed(**kwargs)
            raise ApiError(503, "control plane unavailable")

    def asset_fn(_ctx: AssetContext) -> AssetOut:
        return AssetOut([], row_count=1)

    worker = _bare_worker(client=BrokenClient(), assets={"analytics.daily_sales": asset_fn})

    try:
        worker.handle_dispatch(WorkerDispatchEnvelope.from_dict(_envelope_dict()))
    except ApiError as err:
        assert err.status_code == 503
    else:  # pragma: no cover - defensive
        raise AssertionError("expected completion failure")


def test_heartbeat_interval_uses_budget_and_legacy_fallback() -> None:
    assert _heartbeat_interval_sec(600) == 200.0
    assert _heartbeat_interval_sec(None) == (
        DEFAULT_HEARTBEAT_TIMEOUT_SEC / HEARTBEAT_INTERVAL_DIVISOR
    )
    assert _heartbeat_interval_sec(1) >= 1.0


def test_worker_dispatch_envelope_reads_heartbeat_budget_from_payload() -> None:
    canonical = WorkerDispatchEnvelope.from_dict(_envelope_dict(heartbeat_timeout_sec=600))
    snake = _envelope_dict()
    snake["payload"] = {"heartbeat_timeout_sec": 450}

    assert canonical.heartbeat_timeout_sec == 600
    assert WorkerDispatchEnvelope.from_dict(snake).heartbeat_timeout_sec == 450
    assert WorkerDispatchEnvelope.from_dict(_envelope_dict()).heartbeat_timeout_sec is None
