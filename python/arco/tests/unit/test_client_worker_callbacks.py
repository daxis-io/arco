"""Tests for worker lifecycle callback request shapes."""

from __future__ import annotations

from typing import Any

import pytest

from arco_flow.cli.config import ArcoFlowConfig
from arco_flow.client import ArcoFlowApiClient


def _client_with_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[ArcoFlowApiClient, dict[str, Any]]:
    client = ArcoFlowApiClient(
        ArcoFlowConfig(debug=True, api_url="https://api.example"),
    )
    captured: dict[str, Any] = {}

    def fake_request_json(  # noqa: ANN001
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        base_url: str | None = None,
    ) -> dict[str, Any]:
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = json_body
        captured["params"] = params
        captured["headers"] = headers
        captured["base_url"] = base_url
        return {"ok": True}

    monkeypatch.setattr(client, "_request_json", fake_request_json)
    return client, captured


def test_task_started_uses_opaque_task_id_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, captured = _client_with_capture(monkeypatch)

    client.task_started(
        task_id="ct1_run-123_daily-sales",
        task_key="analytics.daily_sales",
        attempt=1,
        attempt_id="att-1",
        worker_id="worker-1",
        traceparent=None,
        started_at="2026-01-01T00:00:00Z",
        task_token="token",
        callback_base_url="https://callbacks.example",
    )

    assert captured["method"] == "POST"
    assert captured["path"] == "/tasks/ct1_run-123_daily-sales/started"
    assert captured["base_url"] == "https://callbacks.example"


def test_task_started_falls_back_to_task_key_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, captured = _client_with_capture(monkeypatch)

    client.task_started(
        task_key="analytics.daily_sales",
        attempt=1,
        attempt_id="att-1",
        worker_id="worker-1",
        traceparent=None,
        started_at="2026-01-01T00:00:00Z",
        task_token="token",
        callback_base_url="https://callbacks.example",
    )

    assert captured["path"] == "/tasks/analytics.daily_sales/started"


def test_task_completed_uses_task_id_and_omits_missing_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, captured = _client_with_capture(monkeypatch)

    client.task_completed(
        task_id="ct1_run-123_daily-sales",
        task_key="analytics.daily_sales",
        attempt=1,
        attempt_id="att-1",
        worker_id="worker-1",
        traceparent=None,
        outcome="SUCCEEDED",
        completed_at="2026-01-01T00:01:00Z",
        output=None,
        error=None,
        task_token="token",
        callback_base_url="https://callbacks.example",
    )

    body = captured["json_body"]
    assert captured["method"] == "POST"
    assert captured["path"] == "/tasks/ct1_run-123_daily-sales/completed"
    assert isinstance(body, dict)
    assert "output" not in body
    assert "result" not in body
