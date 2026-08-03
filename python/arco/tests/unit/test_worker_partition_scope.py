"""Tests for partition-key propagation into worker execution (issue #339)."""

from __future__ import annotations

import threading
from typing import Any

import pytest

from arco_flow.cli.config import ArcoFlowConfig
from arco_flow.context import AssetContext
from arco_flow.types import AssetOut, PartitionKey
from arco_flow.worker.server import DispatchWorker, WorkerDispatchEnvelope


def _sample_envelope_dict(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
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
    payload.update(overrides)
    return payload


class _FakeClient:
    def __init__(self) -> None:
        self.completed_calls: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    def task_started(self, **kwargs: Any) -> None:
        _ = kwargs

    def task_heartbeat(self, **kwargs: Any) -> None:
        _ = kwargs

    def task_completed(self, **kwargs: Any) -> None:
        with self.lock:
            self.completed_calls.append(kwargs)

    def upload_logs(self, **kwargs: Any) -> None:
        _ = kwargs

    def close(self) -> None:
        return


def _make_worker(
    asset_fn: Any,
    client: Any,
    *,
    partitioned: bool = False,
) -> DispatchWorker:
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
    worker._partitioned_assets = {"analytics.daily_sales"} if partitioned else set()
    worker._init_dispatch_state()
    return worker


def test_worker_executes_with_envelope_partition_key() -> None:
    observed: list[PartitionKey] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        observed.append(ctx.partition_key)
        return AssetOut([], row_count=1)

    client = _FakeClient()
    worker = _make_worker(asset_fn, client, partitioned=True)
    envelope = WorkerDispatchEnvelope.from_dict(
        _sample_envelope_dict(partition_key="date=d:2026-01-01")
    )

    worker.handle_dispatch(envelope)

    assert len(observed) == 1
    assert observed[0].canonical_string() == "date=d:2026-01-01"
    assert client.completed_calls[0]["outcome"] == "SUCCEEDED"


def test_worker_executes_untagged_partition_key_as_string_dimension() -> None:
    observed: list[PartitionKey] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        observed.append(ctx.partition_key)
        return AssetOut([], row_count=1)

    client = _FakeClient()
    worker = _make_worker(asset_fn, client, partitioned=True)
    envelope = WorkerDispatchEnvelope.from_dict(
        _sample_envelope_dict(partition_key="date=2026-01-01")
    )

    worker.handle_dispatch(envelope)

    assert len(observed) == 1
    assert observed[0].dimensions["date"].value == "2026-01-01"


def test_partitioned_asset_without_partition_key_fails_loudly() -> None:
    executions: list[str] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        executions.append(ctx.run_id)
        return AssetOut([], row_count=1)

    client = _FakeClient()
    worker = _make_worker(asset_fn, client, partitioned=True)
    envelope = WorkerDispatchEnvelope.from_dict(_sample_envelope_dict())

    worker.handle_dispatch(envelope)

    assert executions == [], "partitioned asset must not execute unpartitioned"
    assert len(client.completed_calls) == 1
    completed = client.completed_calls[0]
    assert completed["outcome"] == "FAILED"
    assert "partitioned" in completed["error"]["message"]


def test_unpartitioned_asset_without_partition_key_executes_empty_scope() -> None:
    observed: list[PartitionKey] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        observed.append(ctx.partition_key)
        return AssetOut([], row_count=1)

    client = _FakeClient()
    worker = _make_worker(asset_fn, client, partitioned=False)
    envelope = WorkerDispatchEnvelope.from_dict(_sample_envelope_dict())

    worker.handle_dispatch(envelope)

    assert len(observed) == 1
    assert observed[0].canonical_string() == ""
    assert client.completed_calls[0]["outcome"] == "SUCCEEDED"


def test_unparseable_partition_key_fails_the_task() -> None:
    executions: list[str] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        executions.append(ctx.run_id)
        return AssetOut([], row_count=1)

    client = _FakeClient()
    worker = _make_worker(asset_fn, client, partitioned=True)
    envelope = WorkerDispatchEnvelope.from_dict(
        _sample_envelope_dict(partition_key="not-a-pair")
    )

    worker.handle_dispatch(envelope)

    assert executions == []
    assert client.completed_calls[0]["outcome"] == "FAILED"
    assert "partition key" in client.completed_calls[0]["error"]["message"]


@pytest.mark.parametrize(
    "canonical",
    [
        "date=d:2026-01-01",
        "date=d:2026-01-01,region=s:dXMtZWFzdA",
        "",
    ],
)
def test_partition_key_from_canonical_string_round_trips(canonical: str) -> None:
    parsed = PartitionKey.from_canonical_string(canonical)
    assert parsed.canonical_string() == canonical
