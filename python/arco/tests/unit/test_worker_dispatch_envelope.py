"""Tests for worker dispatch envelope handling."""

from __future__ import annotations

import contextlib
import http.client
import json
from types import SimpleNamespace
from typing import Any

from pydantic import SecretStr

from arco_flow.cli.config import ArcoFlowConfig
from arco_flow.context import AssetContext
from arco_flow.types import AssetOut
from arco_flow.worker.server import (
    DispatchHandler,
    DispatchHTTPServer,
    DispatchWorker,
    WorkerDispatchEnvelope,
    _dispatch_authorized,
)


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


def test_worker_dispatch_envelope_accepts_callback_task_id_alias() -> None:
    payload = _sample_canonical_envelope_dict()
    payload.pop("taskId")
    payload["callbackTaskId"] = "ct1_run-123_daily-sales"

    envelope = WorkerDispatchEnvelope.from_dict(payload)

    assert envelope.task_id == "ct1_run-123_daily-sales"
    assert envelope.callback_task_id == "ct1_run-123_daily-sales"


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
        api_url="https://callbacks.example",
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


class _RejectingClient:
    def task_started(self, **kwargs: Any) -> None:
        _ = kwargs
        raise AssertionError("scope mismatch should be rejected before callbacks")

    def task_completed(self, **kwargs: Any) -> None:
        _ = kwargs
        raise AssertionError("scope mismatch should be rejected before callbacks")

    def upload_logs(self, **kwargs: Any) -> None:
        _ = kwargs

    def close(self) -> None:
        return


def _worker_for_scope_validation(*, api_url: str) -> DispatchWorker:
    worker = object.__new__(DispatchWorker)
    worker.config = ArcoFlowConfig(
        debug=True,
        api_url=api_url,
        tenant_id="tenant-a",
        workspace_id="workspace-b",
    )
    worker.worker_id = "worker-1"
    worker._fallback_task_token = "fallback-token"
    worker._client = _RejectingClient()
    worker._assets = {}
    return worker


def test_dispatch_worker_rejects_tenant_scope_mismatch() -> None:
    worker = _worker_for_scope_validation(api_url="https://callbacks.example")
    payload = _sample_envelope_dict()
    payload["tenant_id"] = "tenant-other"
    envelope = WorkerDispatchEnvelope.from_dict(payload)

    try:
        worker.handle_dispatch(envelope)
    except ValueError as err:
        assert "tenant_id" in str(err)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected tenant scope mismatch to be rejected")


def test_dispatch_worker_rejects_workspace_scope_mismatch() -> None:
    worker = _worker_for_scope_validation(api_url="https://callbacks.example")
    payload = _sample_envelope_dict()
    payload["workspace_id"] = "workspace-other"
    envelope = WorkerDispatchEnvelope.from_dict(payload)

    try:
        worker.handle_dispatch(envelope)
    except ValueError as err:
        assert "workspace_id" in str(err)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected workspace scope mismatch to be rejected")


def test_dispatch_worker_rejects_callback_base_url_mismatch() -> None:
    worker = _worker_for_scope_validation(api_url="https://callbacks.example")
    payload = _sample_envelope_dict()
    payload["callback_base_url"] = "https://evil.example"
    envelope = WorkerDispatchEnvelope.from_dict(payload)

    try:
        worker.handle_dispatch(envelope)
    except ValueError as err:
        assert "callback_base_url" in str(err)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected callback base URL mismatch to be rejected")


def test_dispatch_http_rejects_missing_dispatch_authorization() -> None:
    class FakeWorker:
        def __init__(self) -> None:
            self.config = SimpleNamespace(
                tenant_id="tenant-a",
                workspace_id="workspace-b",
                worker_dispatch_secret=SecretStr("dispatch-secret"),
            )
            self.handled = False

        def handle_dispatch(self, dispatch: WorkerDispatchEnvelope) -> None:
            _ = dispatch
            self.handled = True

    worker = FakeWorker()
    with _running_dispatch_server(worker) as port:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connection.request(
            "POST",
            "/dispatch",
            body=json.dumps(_sample_canonical_envelope_dict()),
            headers={"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        status = response.status
        connection.close()

    assert status == 401
    assert worker.handled is False


def test_dispatch_authorization_trims_configured_secret() -> None:
    config = SimpleNamespace(worker_dispatch_secret=SecretStr(" dispatch-secret\n"))

    assert _dispatch_authorized(config, "dispatch-secret", None) is True


def test_dispatch_http_accepts_dispatch_secret_header() -> None:
    class FakeWorker:
        def __init__(self) -> None:
            self.config = SimpleNamespace(
                tenant_id="tenant-a",
                workspace_id="workspace-b",
                worker_dispatch_secret=SecretStr("dispatch-secret"),
            )
            self.handled = False

        def handle_dispatch(self, dispatch: WorkerDispatchEnvelope) -> None:
            assert dispatch.task_id == "ct1_run-123_daily-sales"
            self.handled = True

    worker = FakeWorker()
    with _running_dispatch_server(worker) as port:
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connection.request(
            "POST",
            "/dispatch",
            body=json.dumps(_sample_canonical_envelope_dict()),
            headers={
                "Content-Type": "application/json",
                "X-Arco-Dispatch-Secret": "dispatch-secret",
            },
        )
        response = connection.getresponse()
        body = response.read().decode("utf-8")
        connection.close()

    assert response.status == 200, body
    assert worker.handled is True


@contextlib.contextmanager
def _running_dispatch_server(worker: Any) -> Any:
    server = DispatchHTTPServer(("127.0.0.1", 0), DispatchHandler, worker)
    import threading

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
