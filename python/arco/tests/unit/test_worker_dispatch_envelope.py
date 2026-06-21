"""Tests for worker dispatch envelope handling."""

from __future__ import annotations

from typing import Any

from arco_flow.cli.config import ArcoFlowConfig
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
        "payload": {"partition": "date=2026-01-01"},
    }


def _sample_canonical_envelope_dict() -> dict[str, Any]:
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
        "payload": {"partition": "date=2026-01-01"},
    }


def test_worker_dispatch_envelope_accepts_canonical_task_id() -> None:
    envelope = WorkerDispatchEnvelope.from_dict(_sample_canonical_envelope_dict())

    assert envelope.task_id == "ct1_run-123_daily-sales"
    assert envelope.callback_task_id == "ct1_run-123_daily-sales"
    assert envelope.task_key == "analytics.daily_sales"


def test_worker_dispatch_envelope_accepts_legacy_without_task_id() -> None:
    payload = _sample_envelope_dict()
    payload.pop("task_id")

    envelope = WorkerDispatchEnvelope.from_dict(payload)

    assert envelope.task_id is None
    assert envelope.callback_task_id == "analytics.daily_sales"
    assert envelope.task_key == "analytics.daily_sales"


def test_worker_dispatch_envelope_requires_new_fields() -> None:
    payload = _sample_envelope_dict()
    payload.pop("callback_base_url")

    try:
        WorkerDispatchEnvelope.from_dict(payload)
    except ValueError as err:
        assert "callback_base_url" in str(err)
    else:  # pragma: no cover - defensive
        msg = "expected ValueError for missing callback_base_url"
        raise AssertionError(msg)


def test_dispatch_worker_uses_envelope_token_and_callback_url() -> None:
    class FakeClient:
        def __init__(self) -> None:
            self.started_calls: list[dict[str, Any]] = []
            self.completed_calls: list[dict[str, Any]] = []

        def task_started(self, **kwargs: Any) -> None:
            self.started_calls.append(kwargs)

        def task_completed(self, **kwargs: Any) -> None:
            self.completed_calls.append(kwargs)

        def upload_logs(self, **kwargs: Any) -> None:
            _ = kwargs

        def close(self) -> None:
            return

    def asset_fn(_ctx: AssetContext) -> AssetOut:
        assert _ctx.task_id == "analytics.daily_sales"
        return AssetOut([], row_count=1)

    fake_client = FakeClient()

    worker = object.__new__(DispatchWorker)
    worker.config = ArcoFlowConfig(
        debug=True,
        api_url="https://api.example",
        tenant_id="tenant-a",
        workspace_id="workspace-b",
    )
    worker.worker_id = "worker-1"
    worker._fallback_task_token = "fallback-token"
    worker._client = fake_client
    worker._assets = {"analytics.daily_sales": asset_fn}

    envelope = WorkerDispatchEnvelope.from_dict(_sample_envelope_dict())

    worker.handle_dispatch(envelope)

    assert len(fake_client.started_calls) == 1
    assert len(fake_client.completed_calls) == 1
    assert fake_client.started_calls[0]["task_token"] == "token-from-envelope"
    assert fake_client.completed_calls[0]["task_token"] == "token-from-envelope"
    assert fake_client.started_calls[0]["task_id"] == "ct1_run-123_daily-sales"
    assert fake_client.completed_calls[0]["task_id"] == "ct1_run-123_daily-sales"
    assert fake_client.started_calls[0]["task_key"] == "analytics.daily_sales"
    assert fake_client.completed_calls[0]["task_key"] == "analytics.daily_sales"
    assert fake_client.started_calls[0]["callback_base_url"] == "https://callbacks.example"
    assert fake_client.completed_calls[0]["callback_base_url"] == "https://callbacks.example"


def test_dispatch_worker_rejects_envelope_outside_configured_scope() -> None:
    class FakeClient:
        def task_started(self, **kwargs: Any) -> None:
            _ = kwargs
            raise AssertionError("callback should not start for scope mismatch")

        def task_completed(self, **kwargs: Any) -> None:
            _ = kwargs
            raise AssertionError("callback should not complete for scope mismatch")

        def upload_logs(self, **kwargs: Any) -> None:
            _ = kwargs

        def close(self) -> None:
            return

    worker = object.__new__(DispatchWorker)
    worker.config = ArcoFlowConfig(
        debug=True,
        api_url="https://api.example",
        tenant_id="tenant-a",
        workspace_id="workspace-b",
    )
    worker.worker_id = "worker-1"
    worker._fallback_task_token = "fallback-token"
    worker._client = FakeClient()
    worker._assets = {}

    payload = _sample_envelope_dict()
    payload["tenant_id"] = "tenant-other"
    envelope = WorkerDispatchEnvelope.from_dict(payload)

    try:
        worker.handle_dispatch(envelope)
    except ValueError as err:
        assert "tenant_id mismatch" in str(err)
    else:  # pragma: no cover - defensive
        msg = "expected ValueError for tenant scope mismatch"
        raise AssertionError(msg)
