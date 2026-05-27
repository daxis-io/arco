"""Tests for worker heartbeat callback compatibility."""

from __future__ import annotations

import json
from typing import Any

import httpx

from arco_flow.cli.config import ArcoFlowConfig
from arco_flow.client import ArcoFlowApiClient


def test_task_heartbeat_parses_canonical_json_response() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content.decode("utf-8"))
        return httpx.Response(
            200,
            json={
                "acknowledged": True,
                "shouldCancel": True,
                "serverTime": "2026-01-01T00:00:01Z",
            },
        )

    client = ArcoFlowApiClient(ArcoFlowConfig(debug=True, api_url="https://api.example"))
    client._client = httpx.Client(transport=httpx.MockTransport(handler))

    response = client.task_heartbeat(
        task_id="ct1_run-123_daily-sales",
        task_key="analytics.daily_sales",
        attempt=1,
        attempt_id="att-1",
        worker_id="worker-1",
        traceparent="00-trace",
        task_token="token",
        callback_base_url="https://callbacks.example",
        progress_pct=50,
        message="halfway",
    )

    assert (
        captured["url"]
        == "https://callbacks.example/api/v1/tasks/ct1_run-123_daily-sales/heartbeat"
    )
    assert captured["body"]["progressPct"] == 50
    assert response.payload["acknowledged"] is True
    assert response.payload["shouldCancel"] is True
    assert response.payload["serverTime"] == "2026-01-01T00:00:01Z"


def test_task_heartbeat_empty_success_response_means_continue() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(204, content=b"")

    client = ArcoFlowApiClient(ArcoFlowConfig(debug=True, api_url="https://api.example"))
    client._client = httpx.Client(transport=httpx.MockTransport(handler))

    response = client.task_heartbeat(
        task_id="ct1_run-123_daily-sales",
        task_key="analytics.daily_sales",
        attempt=1,
        attempt_id="att-1",
        worker_id="worker-1",
        traceparent=None,
        task_token="token",
        callback_base_url="https://callbacks.example",
    )

    assert response.payload == {
        "acknowledged": True,
        "shouldCancel": False,
    }


def test_task_heartbeat_rejects_non_integer_progress() -> None:
    client = ArcoFlowApiClient(ArcoFlowConfig(debug=True, api_url="https://api.example"))

    try:
        client.task_heartbeat(
            task_id="ct1_run-123_daily-sales",
            attempt=1,
            attempt_id="att-1",
            worker_id="worker-1",
            traceparent=None,
            task_token="token",
            progress_pct=50.5,  # type: ignore[arg-type]
        )
    except ValueError as err:
        assert "progress_pct" in str(err)
    else:
        raise AssertionError("expected non-integer progress_pct to be rejected")


def test_task_heartbeat_rejects_out_of_range_progress() -> None:
    client = ArcoFlowApiClient(ArcoFlowConfig(debug=True, api_url="https://api.example"))

    try:
        client.task_heartbeat(
            task_id="ct1_run-123_daily-sales",
            attempt=1,
            attempt_id="att-1",
            worker_id="worker-1",
            traceparent=None,
            task_token="token",
            progress_pct=101,
        )
    except ValueError as err:
        assert "between 0 and 100" in str(err)
    else:
        raise AssertionError("expected out-of-range progress_pct to be rejected")
