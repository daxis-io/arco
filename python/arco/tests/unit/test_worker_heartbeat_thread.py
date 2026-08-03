"""Tests for periodic worker heartbeats during task execution (issue #367)."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from arco_flow.cli.config import ArcoFlowConfig
from arco_flow.context import AssetContext
from arco_flow.types import AssetOut
from arco_flow.worker.server import (
    HEARTBEAT_MAX_INTERVAL_SECONDS,
    MIN_HEARTBEAT_TIMEOUT_SECONDS,
    DispatchWorker,
    HeartbeatSender,
    WorkerDispatchEnvelope,
    heartbeat_interval_seconds,
)


def test_heartbeat_interval_is_well_under_staleness_threshold() -> None:
    # Planner default: 300s timeout -> 60s interval, far inside the
    # 300s + 30s force-fail window enforced by anti-entropy.
    assert heartbeat_interval_seconds(300) == 60.0
    # Legacy envelopes without the field assume the planner default.
    assert heartbeat_interval_seconds(None) == 60.0
    # The shared floor is honoured exactly.
    assert heartbeat_interval_seconds(MIN_HEARTBEAT_TIMEOUT_SECONDS) > 0.0
    # Long timeouts stay clamped so failures are still detected promptly.
    assert heartbeat_interval_seconds(3600) == HEARTBEAT_MAX_INTERVAL_SECONDS


def test_sub_floor_heartbeat_timeout_is_rejected_rather_than_reinterpreted() -> None:
    """Zero and other sub-floor timeouts meant two different things.

    The control plane reaps a RUNNING task after
    `heartbeat_timeout_sec + 30s` of silence, so zero left a 30-second window;
    this worker read a falsy timeout as "missing" and heartbeated on the
    300-second default, so the task was reaped about a minute before its first
    heartbeat. Both sides now share `MIN_HEARTBEAT_TIMEOUT_SECONDS`: the
    planner never emits a lower value, and an envelope carrying one is
    malformed here instead of being silently reinterpreted.
    """
    for malformed in (0, 1, MIN_HEARTBEAT_TIMEOUT_SECONDS - 1):
        with pytest.raises(ValueError, match="below the"):
            heartbeat_interval_seconds(malformed)


def test_heartbeat_sender_posts_periodically_until_stopped() -> None:
    beats = threading.Semaphore(0)

    sender = HeartbeatSender(post=beats.release, interval_seconds=0.01)
    sender.start()

    assert beats.acquire(timeout=5), "first heartbeat never posted"
    assert beats.acquire(timeout=5), "second heartbeat never posted"

    sender.stop()

    # Drain anything posted before stop, then verify the loop has exited.
    while beats.acquire(blocking=False):
        pass
    time.sleep(0.1)
    assert not beats.acquire(blocking=False), "heartbeat posted after stop"


def test_heartbeat_errors_never_propagate() -> None:
    calls: list[int] = []

    def failing_post() -> None:
        calls.append(1)
        msg = "callback endpoint unavailable"
        raise RuntimeError(msg)

    sender = HeartbeatSender(post=failing_post, interval_seconds=0.01)
    sender.start()

    deadline = time.monotonic() + 5
    while len(calls) < 3 and time.monotonic() < deadline:
        time.sleep(0.01)

    sender.stop()

    assert len(calls) >= 3, "heartbeat loop stopped after an error"


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
        "heartbeat_timeout_sec": 300,
        "worker_queue": "default-queue",
        "callback_base_url": "https://callbacks.example",
        "task_token": "token-from-envelope",
        "token_expires_at": "2026-01-01T00:00:00Z",
        "traceparent": None,
        "payload": {},
    }


class _HeartbeatRecordingClient:
    def __init__(self) -> None:
        self.heartbeat_calls: list[dict[str, Any]] = []
        self.completed_calls: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    def task_started(self, **kwargs: Any) -> None:
        _ = kwargs

    def task_heartbeat(self, **kwargs: Any) -> None:
        with self.lock:
            self.heartbeat_calls.append(kwargs)

    def task_completed(self, **kwargs: Any) -> None:
        with self.lock:
            self.completed_calls.append(kwargs)

    def upload_logs(self, **kwargs: Any) -> None:
        _ = kwargs

    def close(self) -> None:
        return


def test_handle_dispatch_heartbeats_during_long_asset_execution(
    monkeypatch: Any,
) -> None:
    # Shrink the interval so the test observes several heartbeats quickly.
    import arco_flow.worker.server as server_module

    monkeypatch.setattr(
        server_module,
        "heartbeat_interval_seconds",
        lambda _timeout: 0.01,
    )

    client = _HeartbeatRecordingClient()
    seen_enough = threading.Event()

    original_heartbeat = client.task_heartbeat

    def counting_heartbeat(**kwargs: Any) -> None:
        original_heartbeat(**kwargs)
        if len(client.heartbeat_calls) >= 2:
            seen_enough.set()

    client.task_heartbeat = counting_heartbeat  # type: ignore[method-assign]

    def slow_asset(_ctx: AssetContext) -> AssetOut:
        assert seen_enough.wait(timeout=10), "no heartbeats observed during execution"
        return AssetOut([], row_count=1)

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
    worker._assets = {"analytics.daily_sales": slow_asset}
    worker._partitioned_assets = set()
    worker._init_dispatch_state()

    envelope = WorkerDispatchEnvelope.from_dict(_sample_envelope_dict())
    worker.handle_dispatch(envelope)

    assert len(client.heartbeat_calls) >= 2
    first = client.heartbeat_calls[0]
    assert first["task_id"] == "ct1_run-123_daily-sales"
    assert first["attempt"] == 1
    assert first["attempt_id"] == "att-1"
    assert first["task_token"] == "token-from-envelope"
    assert first["callback_base_url"] == "https://callbacks.example"
    assert len(client.completed_calls) == 1
    assert client.completed_calls[0]["outcome"] == "SUCCEEDED"


def test_handle_dispatch_succeeds_when_heartbeats_fail(monkeypatch: Any) -> None:
    import arco_flow.worker.server as server_module

    monkeypatch.setattr(
        server_module,
        "heartbeat_interval_seconds",
        lambda _timeout: 0.01,
    )

    heartbeat_attempts: list[int] = []
    attempts_lock = threading.Lock()
    attempted = threading.Event()

    class _FailingHeartbeatClient(_HeartbeatRecordingClient):
        def task_heartbeat(self, **kwargs: Any) -> None:
            _ = kwargs
            with attempts_lock:
                heartbeat_attempts.append(1)
            attempted.set()
            msg = "heartbeat endpoint down"
            raise RuntimeError(msg)

    client = _FailingHeartbeatClient()

    def slow_asset(_ctx: AssetContext) -> AssetOut:
        assert attempted.wait(timeout=10), "heartbeat was never attempted"
        return AssetOut([], row_count=1)

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
    worker._assets = {"analytics.daily_sales": slow_asset}
    worker._partitioned_assets = set()
    worker._init_dispatch_state()

    envelope = WorkerDispatchEnvelope.from_dict(_sample_envelope_dict())
    worker.handle_dispatch(envelope)

    assert heartbeat_attempts, "heartbeat should have been attempted"
    assert len(client.completed_calls) == 1
    assert client.completed_calls[0]["outcome"] == "SUCCEEDED"
