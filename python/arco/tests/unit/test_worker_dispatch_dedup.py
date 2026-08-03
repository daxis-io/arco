"""Tests for dispatch_id deduplication in the worker (issue #328)."""

from __future__ import annotations

import contextlib
import http.client
import json
import threading
from typing import TYPE_CHECKING, Any

import pytest
from pydantic import SecretStr

if TYPE_CHECKING:
    from collections.abc import Iterator

from arco_flow.cli.config import ArcoFlowConfig
from arco_flow.client import ApiError
from arco_flow.context import AssetContext
from arco_flow.types import AssetOut
from arco_flow.worker.server import (
    DispatchHandler,
    DispatchHTTPServer,
    DispatchOutcome,
    DispatchWorker,
    WorkerDispatchEnvelope,
)

DISPATCH_SECRET = "dispatch-secret"  # noqa: S105


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
        worker_dispatch_secret=SecretStr(DISPATCH_SECRET),
    )
    worker.worker_id = "worker-1"
    worker._fallback_task_token = "fallback-token"
    worker._client = client
    worker._assets = {"analytics.daily_sales": asset_fn}
    worker._partitioned_assets = set()
    worker._init_dispatch_state()
    return worker


@contextlib.contextmanager
def _serving(worker: DispatchWorker) -> Iterator[int]:
    """Run the worker's real HTTP server on an ephemeral loopback port."""
    server = DispatchHTTPServer(("127.0.0.1", 0), DispatchHandler, worker)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield int(server.server_port)
    finally:
        server.shutdown()
        thread.join(timeout=10)
        server.server_close()


def _post_dispatch(port: int, payload: dict[str, Any]) -> int:
    """POST one dispatch delivery and return the HTTP status code."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        connection.request(
            "POST",
            "/dispatch",
            body=json.dumps(payload),
            headers={
                "Content-Type": "application/json",
                "X-Arco-Dispatch-Secret": DISPATCH_SECRET,
            },
        )
        response = connection.getresponse()
        response.read()
        return int(response.status)
    finally:
        connection.close()


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


def test_inflight_redelivery_gets_a_retryable_status_over_real_http() -> None:
    """H6: an in-flight duplicate must not be told it succeeded.

    `handle_dispatch` returning normally for an in-flight duplicate made the
    HTTP handler answer 200, so Cloud Tasks stopped redelivering. If the
    executing request then crashed before reporting anything, nothing was left
    to drive the attempt. The duplicate must instead get a retryable status,
    and once the owner has failed, a further delivery must actually execute.
    """
    first_started = threading.Event()
    release_first = threading.Event()
    executions: list[str] = []
    execution_lock = threading.Lock()

    def asset_fn(ctx: AssetContext) -> AssetOut:
        with execution_lock:
            executions.append(ctx.run_id)
        first_started.set()
        assert release_first.wait(timeout=30), "test deadlock: release never signalled"
        msg = "owner crashed before reporting"
        raise RuntimeError(msg)

    client = _RecordingClient()
    worker = _make_worker(asset_fn, client)
    envelope = _sample_envelope_dict()
    first_status: list[int] = []

    with _serving(worker) as port:
        owner = threading.Thread(
            target=lambda: first_status.append(_post_dispatch(port, envelope))
        )
        owner.start()
        assert first_started.wait(timeout=30), "the owning delivery never started"

        try:
            assert _post_dispatch(port, envelope) == 503, (
                "an in-flight duplicate must be retryable, not acknowledged"
            )
        finally:
            release_first.set()
            owner.join(timeout=30)

        assert not owner.is_alive()
        # The asset raised, but the worker still reports a terminal FAILED
        # outcome, so the owning delivery itself is a completed dispatch.
        assert first_status == [200]
        assert len(executions) == 1

        # A delivery after the owner finished is a completed duplicate and is
        # acknowledged without re-execution.
        assert _post_dispatch(port, envelope) == 200
        assert len(executions) == 1

    assert len(client.completed_calls) == 1
    assert client.completed_calls[0]["outcome"] == "FAILED"


class _AlreadyTerminalStartedClient(_RecordingClient):
    """Control plane that reports the attempt as already terminal."""

    def __init__(self, error_code: str) -> None:
        super().__init__()
        self._error_code = error_code

    def task_started(self, **kwargs: Any) -> None:
        with self.lock:
            self.started_calls.append(kwargs)
        raise ApiError(409, self._error_code)


@pytest.mark.parametrize(
    "error_code",
    ["task_already_terminal", "attempt_already_completed", "attempt_mismatch"],
)
def test_worker_reconstruction_after_durable_completion_does_not_re_execute(
    error_code: str,
) -> None:
    """H5: process-local dedup cannot cover the callback/record crash window.

    The first process posts a completion that the control plane durably
    accepts, then dies before recording the dispatch id (or after recording it
    but before its HTTP 200, then restarts). The replacement worker has an
    empty recent set, so only the control plane can stop the redelivery from
    re-running the asset — and it does, on `task_started`.
    """
    executions: list[str] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        executions.append(ctx.run_id)
        return AssetOut([], row_count=1)

    # A fresh worker object models the reconstructed process: no memory at all.
    client = _AlreadyTerminalStartedClient(error_code)
    reconstructed = _make_worker(asset_fn, client)
    assert not reconstructed._recent_dispatch_ids
    assert not reconstructed._inflight_dispatch_ids

    envelope = _sample_envelope_dict()
    with _serving(reconstructed) as port:
        assert _post_dispatch(port, envelope) == 200, (
            "an already-terminal attempt must be acknowledged, not retried forever"
        )
        # A further redelivery is now also covered by the local fast path.
        assert _post_dispatch(port, envelope) == 200

    assert executions == [], "the asset must not run a second time"
    assert client.completed_calls == [], "no second completion may be reported"
    assert len(client.started_calls) == 1, (
        "the second delivery is served from the local fast path"
    )


def test_transient_started_failure_still_surfaces_as_a_retryable_error() -> None:
    """A non-terminal control-plane failure must not be mistaken for dedup."""
    executions: list[str] = []

    def asset_fn(ctx: AssetContext) -> AssetOut:
        executions.append(ctx.run_id)
        return AssetOut([], row_count=1)

    client = _FailFirstStartedClient()
    worker = _make_worker(asset_fn, client)
    envelope = _sample_envelope_dict()

    with _serving(worker) as port:
        assert _post_dispatch(port, envelope) == 500
        assert executions == []
        # The redelivery must re-execute: nothing terminal was recorded.
        assert _post_dispatch(port, envelope) == 200

    assert len(executions) == 1


def test_handle_dispatch_reports_distinct_outcomes() -> None:
    """The transport-facing outcome must distinguish the three cases."""

    def asset_fn(_ctx: AssetContext) -> AssetOut:
        return AssetOut([], row_count=1)

    worker = _make_worker(asset_fn, _RecordingClient())
    envelope = WorkerDispatchEnvelope.from_dict(_sample_envelope_dict())

    assert worker.handle_dispatch(envelope) is DispatchOutcome.EXECUTED
    assert worker.handle_dispatch(envelope) is DispatchOutcome.ALREADY_TERMINAL


class _FailCompletionClient(_RecordingClient):
    """Control plane whose completion callback fails, as on an owner crash."""

    def task_completed(self, **kwargs: Any) -> None:
        with self.lock:
            self.completed_calls.append(kwargs)
        msg = "owner died before its terminal report was accepted"
        raise ApiError(500, msg)


def test_owner_that_never_reports_terminal_leaves_the_work_redeliverable() -> None:
    """H6: the retryable duplicate must still be covered when the owner dies.

    The redelivery is told to retry while the owner executes. The owner then
    fails without a durable terminal report, so nothing about the attempt is
    known — a later delivery must execute rather than be duplicate-acked.
    """
    first_started = threading.Event()
    release_first = threading.Event()
    executions: list[str] = []
    execution_lock = threading.Lock()

    def asset_fn(ctx: AssetContext) -> AssetOut:
        with execution_lock:
            executions.append(ctx.run_id)
        first_started.set()
        assert release_first.wait(timeout=30), "test deadlock: release never signalled"
        return AssetOut([], row_count=1)

    client = _FailCompletionClient()
    worker = _make_worker(asset_fn, client)
    envelope = _sample_envelope_dict()
    owner_status: list[int] = []

    with _serving(worker) as port:
        owner = threading.Thread(
            target=lambda: owner_status.append(_post_dispatch(port, envelope))
        )
        owner.start()
        assert first_started.wait(timeout=30), "the owning delivery never started"

        try:
            assert _post_dispatch(port, envelope) == 503
        finally:
            release_first.set()
            owner.join(timeout=30)

        assert owner_status == [500], "the owner must report its own failure"
        assert len(executions) == 1

        # Nothing terminal was recorded, so the next delivery must execute.
        assert _post_dispatch(port, envelope) == 500
        assert len(executions) == 2
