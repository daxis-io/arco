"""Tests for API client request shapes."""

from __future__ import annotations

import pytest


def test_trigger_run_uses_partition_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from arco_flow.cli.config import ArcoFlowConfig
    from arco_flow.client import ArcoFlowApiClient

    config = ArcoFlowConfig(debug=True, api_url="https://example.invalid")
    client = ArcoFlowApiClient(config)

    captured: dict[str, object] = {}

    def fake_request_json(  # noqa: ANN001
        method: str,
        path: str,
        *,
        json_body: dict[str, object] | None = None,
        params: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, object]:
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = json_body
        captured["params"] = params
        captured["headers"] = headers
        return {}

    monkeypatch.setattr(client, "_request_json", fake_request_json)

    client.trigger_run(
        workspace_id="ws",
        selection=["raw.events"],
        partition_key="date=s:MjAyNS0wMS0xNQ",
        run_key="rk-001",
    )

    body = captured["json_body"]
    assert isinstance(body, dict)
    assert body["selection"] == ["raw.events"]
    assert body["partitionKey"] == "date=s:MjAyNS0wMS0xNQ"
    assert body["runKey"] == "rk-001"
    assert "partitions" not in body


def test_trigger_run_rejects_both_partition_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    from arco_flow.cli.config import ArcoFlowConfig
    from arco_flow.client import ArcoFlowApiClient

    config = ArcoFlowConfig(debug=True, api_url="https://example.invalid")
    client = ArcoFlowApiClient(config)

    monkeypatch.setattr(client, "_request_json", lambda *args, **kwargs: {})

    with pytest.raises(ValueError, match="either partition_key or partitions"):
        client.trigger_run(
            workspace_id="ws",
            selection=["raw.events"],
            partitions=[{"key": "date", "value": "2025-01-15"}],
            partition_key="date=s:MjAyNS0wMS0xNQ",
        )


def test_trigger_run_rejects_empty_partition_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from arco_flow.cli.config import ArcoFlowConfig
    from arco_flow.client import ArcoFlowApiClient

    config = ArcoFlowConfig(debug=True, api_url="https://example.invalid")
    client = ArcoFlowApiClient(config)

    monkeypatch.setattr(client, "_request_json", lambda *args, **kwargs: {})

    with pytest.raises(ValueError, match="partition_key cannot be empty"):
        client.trigger_run(
            workspace_id="ws",
            selection=["raw.events"],
            partition_key="",
        )
