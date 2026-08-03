"""Minimal worker HTTP server for executing dispatched tasks."""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import io
import json
import os
import socket
import threading
import traceback
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

if TYPE_CHECKING:
    from collections.abc import Callable

from rich.console import Console

from arco_flow.cli.config import ArcoFlowConfig, get_config
from arco_flow.client import ApiError, ArcoFlowApiClient
from arco_flow.context import AssetContext
from arco_flow.manifest.discovery import AssetDiscovery, AssetDiscoveryError
from arco_flow.types import AssetOut, PartitionKey

console = Console()
err_console = Console(stderr=True)

DISPATCH_SECRET_HEADER = "X-Arco-Dispatch-Secret"

# Heartbeat timing. The control plane force-fails a RUNNING task after
# `heartbeat_timeout_sec` (300s at the planner default) plus a 30s grace
# (`RUNNING_TASK_STALENESS_GRACE` in
# crates/arco-flow/src/orchestration/controllers/anti_entropy.rs). The worker
# heartbeats at `heartbeat_timeout_sec / HEARTBEAT_INTERVAL_DIVISOR`, clamped
# to [HEARTBEAT_MIN_INTERVAL_SECONDS, HEARTBEAT_MAX_INTERVAL_SECONDS] — 60s at
# defaults — so several consecutive heartbeats must be lost before the
# force-fail window can expire (issue #367).
DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 300
HEARTBEAT_INTERVAL_DIVISOR = 5
HEARTBEAT_MIN_INTERVAL_SECONDS = 5.0
HEARTBEAT_MAX_INTERVAL_SECONDS = 60.0

# Smallest heartbeat timeout the control plane will plan, mirroring
# `MIN_HEARTBEAT_TIMEOUT_SEC` in
# crates/arco-flow/src/orchestration/controllers/run_request_processor.rs.
#
# Zero used to be representable and meant two different things: the control
# plane read it as "reap after the 30s grace alone", while this worker read
# `heartbeat_timeout_sec or DEFAULT` and heartbeated as if the timeout were
# 300s. A task planned with zero was therefore reaped roughly a minute before
# its worker's first heartbeat. The planner now refuses to emit a value below
# this floor, and an envelope carrying one is treated as malformed here rather
# than being silently reinterpreted.
MIN_HEARTBEAT_TIMEOUT_SECONDS = 30

# Bounded memory for dispatch deduplication (issue #328).
RECENT_DISPATCH_LIMIT = 1024

# Control-plane error codes that authoritatively say "this attempt is finished".
#
# Process-local dedup cannot cover the crash window between a durable
# `task_completed` and the worker recording it: the replacement process starts
# with an empty set and would re-execute the asset on redelivery. Only the
# control plane knows the attempt already reported a terminal result, and it
# says so on `task_started`.
TERMINAL_ATTEMPT_CONFLICT_STATUS = 409
TERMINAL_ATTEMPT_ERROR_CODES = frozenset(
    {
        "task_already_terminal",
        "attempt_already_completed",
        "attempt_mismatch",
        "attempt_id_mismatch",
    }
)


class DispatchClaim(Enum):
    """Outcome of atomically classifying a dispatch id for execution."""

    CLAIMED = "claimed"
    """No other delivery holds this dispatch id; this delivery executes."""

    IN_FLIGHT = "in_flight"
    """Another delivery is executing it right now; its outcome is unknown."""

    COMPLETED = "completed"
    """A previous delivery reported a terminal result for it."""


class DispatchOutcome(Enum):
    """What the HTTP transport must do after a dispatch delivery."""

    EXECUTED = "executed"
    """The asset ran and its terminal result was reported."""

    ALREADY_TERMINAL = "already_terminal"
    """The attempt was already finished; acknowledge without re-executing."""

    RETRY_LATER = "retry_later"
    """The owner's outcome is not known yet; the delivery must be retried."""


def heartbeat_interval_seconds(heartbeat_timeout_sec: int | None) -> float:
    """Choose the heartbeat interval for a task attempt.

    Args:
        heartbeat_timeout_sec: Timeout carried by the dispatch envelope, or
            None for envelopes from older control planes (the planner default
            of 300s is assumed).

    Returns:
        Seconds between heartbeats, well under the staleness threshold.

    Raises:
        ValueError: If the envelope carries a timeout below
            `MIN_HEARTBEAT_TIMEOUT_SECONDS`. Such a value has no consistent
            meaning across the control plane and this worker, so it is
            rejected rather than reinterpreted.
    """
    if heartbeat_timeout_sec is None:
        timeout = DEFAULT_HEARTBEAT_TIMEOUT_SECONDS
    elif heartbeat_timeout_sec < MIN_HEARTBEAT_TIMEOUT_SECONDS:
        msg = (
            f"heartbeat_timeout_sec {heartbeat_timeout_sec} is below the "
            f"{MIN_HEARTBEAT_TIMEOUT_SECONDS}s floor shared with the control plane; "
            "refusing to guess an interval the reaper does not agree with"
        )
        raise ValueError(msg)
    else:
        timeout = heartbeat_timeout_sec
    interval = timeout / HEARTBEAT_INTERVAL_DIVISOR
    return min(HEARTBEAT_MAX_INTERVAL_SECONDS, max(HEARTBEAT_MIN_INTERVAL_SECONDS, interval))


class HeartbeatSender:
    """Posts periodic task heartbeats on a daemon thread while an asset runs.

    Heartbeats are advisory: any error is logged and swallowed so a heartbeat
    failure can never fail the task itself.
    """

    def __init__(self, post: Callable[[], None], interval_seconds: float) -> None:
        """Initialize the sender.

        Args:
            post: Callable that sends one heartbeat.
            interval_seconds: Seconds between heartbeats.
        """
        self._post = post
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="arco-task-heartbeat",
        )

    def start(self) -> None:
        """Start heartbeating in the background."""
        self._thread.start()

    def stop(self, timeout_seconds: float = 5.0) -> None:
        """Stop heartbeating and wait briefly for the thread to exit.

        Args:
            timeout_seconds: Maximum time to wait for the thread.
        """
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=timeout_seconds)

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self._post()
            except Exception as exc:  # heartbeats are advisory; never fail the task
                err_console.print(f"[yellow]![/yellow] Task heartbeat failed: {exc}")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _is_terminal_attempt_conflict(error: ApiError) -> bool:
    """Whether the control plane says this attempt already finished.

    Args:
        error: The failure raised by a `task_started` callback.

    Returns:
        True when the response is a 409 whose error code names a durable
        terminal or superseded attempt. The worker must then skip execution:
        the asset already ran, and re-running it would duplicate side effects
        that the control plane has already recorded as complete.
    """
    if error.status_code != TERMINAL_ATTEMPT_CONFLICT_STATUS:
        return False
    message = str(error)
    return any(code in message for code in TERMINAL_ATTEMPT_ERROR_CODES)


def _get_field(payload: dict[str, Any], snake: str, camel: str) -> Any:
    if snake in payload:
        return payload[snake]
    return payload.get(camel)


@dataclass
class DispatchPayload:
    run_id: str
    task_key: str
    attempt: int
    attempt_id: str
    traceparent: str | None
    task_token: str | None
    token_expires_at: str | None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> DispatchPayload:
        run_id = _get_field(payload, "run_id", "runId")
        task_key = _get_field(payload, "task_key", "taskKey")
        attempt = _get_field(payload, "attempt", "attempt")
        attempt_id = _get_field(payload, "attempt_id", "attemptId")
        traceparent = _get_field(payload, "traceparent", "traceparent")
        task_token = _get_field(payload, "task_token", "taskToken")
        token_expires_at = _get_field(payload, "token_expires_at", "tokenExpiresAt")

        if not run_id or not task_key or not attempt or not attempt_id:
            msg = "dispatch payload missing required fields"
            raise ValueError(msg)

        return cls(
            run_id=str(run_id),
            task_key=str(task_key),
            attempt=int(attempt),
            attempt_id=str(attempt_id),
            traceparent=str(traceparent) if traceparent else None,
            task_token=str(task_token) if task_token else None,
            token_expires_at=str(token_expires_at) if token_expires_at else None,
        )


def _select_task_token(payload_token: str | None, fallback_token: str) -> str:
    if payload_token and payload_token.strip():
        return payload_token
    return fallback_token


def _secret_value(secret: Any) -> str:
    if secret is None:
        return ""
    get_secret_value = getattr(secret, "get_secret_value", None)
    if callable(get_secret_value):
        return str(get_secret_value() or "")
    return str(secret)


def _extract_bearer_token(header: str | None) -> str | None:
    if not header:
        return None
    scheme, separator, token = header.partition(" ")
    if not separator or scheme.lower() != "bearer":
        return None
    token = token.strip()
    return token or None


def _dispatch_authorized(
    config: Any,
    dispatch_secret_header: str | None,
    authorization_header: str | None,
) -> bool:
    expected = _secret_value(getattr(config, "worker_dispatch_secret", None)).strip()
    if not expected:
        return False
    if dispatch_secret_header and hmac.compare_digest(dispatch_secret_header.strip(), expected):
        return True
    token = _extract_bearer_token(authorization_header)
    if token is None:
        return False
    return hmac.compare_digest(token, expected)


def _normalize_base_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


@dataclass
class WorkerDispatchEnvelope:
    tenant_id: str
    workspace_id: str
    run_id: str
    task_id: str | None
    task_key: str
    attempt: int
    attempt_id: str
    dispatch_id: str
    partition_key: str | None
    heartbeat_timeout_sec: int | None
    worker_queue: str
    callback_base_url: str
    task_token: str
    token_expires_at: str
    traceparent: str | None
    payload: Any

    @property
    def callback_task_id(self) -> str:
        return self.task_id or self.task_key

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> WorkerDispatchEnvelope:
        tenant_id = _get_field(payload, "tenant_id", "tenantId")
        workspace_id = _get_field(payload, "workspace_id", "workspaceId")
        run_id = _get_field(payload, "run_id", "runId")
        task_id = _get_field(payload, "task_id", "taskId")
        callback_task_id = _get_field(payload, "callback_task_id", "callbackTaskId")
        task_key = _get_field(payload, "task_key", "taskKey")
        attempt = _get_field(payload, "attempt", "attempt")
        attempt_id = _get_field(payload, "attempt_id", "attemptId")
        dispatch_id = _get_field(payload, "dispatch_id", "dispatchId")
        partition_key = _get_field(payload, "partition_key", "partitionKey")
        heartbeat_timeout_sec = _get_field(payload, "heartbeat_timeout_sec", "heartbeatTimeoutSec")
        worker_queue = _get_field(payload, "worker_queue", "workerQueue")
        callback_base_url = _get_field(payload, "callback_base_url", "callbackBaseUrl")
        task_token = _get_field(payload, "task_token", "taskToken")
        token_expires_at = _get_field(payload, "token_expires_at", "tokenExpiresAt")
        traceparent = _get_field(payload, "traceparent", "traceparent")
        worker_payload = _get_field(payload, "payload", "payload")

        required = {
            "tenant_id": tenant_id,
            "workspace_id": workspace_id,
            "run_id": run_id,
            "task_key": task_key,
            "attempt": attempt,
            "attempt_id": attempt_id,
            "dispatch_id": dispatch_id,
            "worker_queue": worker_queue,
            "callback_base_url": callback_base_url,
            "task_token": task_token,
            "token_expires_at": token_expires_at,
            "payload": worker_payload,
        }
        missing = [key for key, value in required.items() if value is None or value == ""]
        if missing:
            msg = "dispatch payload missing required fields"
            raise ValueError(f"{msg}: {', '.join(missing)}")

        if task_id and callback_task_id and task_id != callback_task_id:
            msg = "dispatch payload has conflicting task_id and callback_task_id"
            raise ValueError(msg)
        resolved_task_id = task_id or callback_task_id

        return cls(
            tenant_id=str(tenant_id),
            workspace_id=str(workspace_id),
            run_id=str(run_id),
            task_id=str(resolved_task_id) if resolved_task_id else None,
            task_key=str(task_key),
            attempt=int(attempt),
            attempt_id=str(attempt_id),
            dispatch_id=str(dispatch_id),
            partition_key=str(partition_key) if partition_key else None,
            heartbeat_timeout_sec=(
                int(heartbeat_timeout_sec) if heartbeat_timeout_sec is not None else None
            ),
            worker_queue=str(worker_queue),
            callback_base_url=str(callback_base_url),
            task_token=str(task_token),
            token_expires_at=str(token_expires_at),
            traceparent=str(traceparent) if traceparent else None,
            payload=worker_payload,
        )


class DispatchWorker:
    """Executes dispatched tasks and reports lifecycle callbacks."""

    def __init__(
        self,
        config: ArcoFlowConfig,
        *,
        root_path: Path,
        worker_id: str | None = None,
    ) -> None:
        self.config = config
        self.worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}"
        self._fallback_task_token = (
            config.task_token.get_secret_value() or config.api_key.get_secret_value() or "debug"
        )
        self._client = ArcoFlowApiClient(config)
        self._assets: dict[str, Any] = {}
        self._partitioned_assets: set[str] = set()
        self._load_assets(root_path)
        self._init_dispatch_state()

    def _init_dispatch_state(self) -> None:
        """Initialize the dispatch_id deduplication registry (issue #328)."""
        self._dedup_lock = threading.Lock()
        self._inflight_dispatch_ids: set[str] = set()
        self._recent_dispatch_ids: OrderedDict[str, None] = OrderedDict()

    def _load_assets(self, root_path: Path) -> None:
        discovery = AssetDiscovery(root_path=root_path)
        try:
            assets = discovery.discover(strict=True)
        except AssetDiscoveryError as err:
            err_console.print("[red]✗[/red] Asset discovery failed")
            for failure in err.failures:
                err_console.print(f"  - {failure.file_path}: {failure.error}")
            raise SystemExit(1) from err

        self._assets = {str(asset.key): asset.func for asset in assets}
        self._partitioned_assets = {
            str(asset.key) for asset in assets if asset.definition.partitioning.is_partitioned
        }

    def close(self) -> None:
        """Close worker resources."""
        self._client.close()

    def handle_dispatch(self, payload: WorkerDispatchEnvelope) -> DispatchOutcome:
        """Execute one dispatch delivery, or explain why it was not executed.

        Returns:
            The outcome the HTTP transport must translate into a status code.
            A `RETRY_LATER` outcome must not be acknowledged as success: the
            executing delivery may still crash before reporting anything.
        """
        self._validate_dispatch_envelope(payload)
        claim = self._begin_dispatch(payload.dispatch_id)
        if claim is DispatchClaim.COMPLETED:
            console.print(
                f"[blue]i[/blue] Dispatch {payload.dispatch_id} already reported a "
                "terminal result; acknowledged without re-execution"
            )
            return DispatchOutcome.ALREADY_TERMINAL
        if claim is DispatchClaim.IN_FLIGHT:
            console.print(
                f"[blue]i[/blue] Dispatch {payload.dispatch_id} is still executing; "
                "asking for redelivery instead of acknowledging an unknown outcome"
            )
            return DispatchOutcome.RETRY_LATER
        try:
            return self._run_dispatch(payload)
        finally:
            self._release_dispatch(payload.dispatch_id)

    def _begin_dispatch(self, dispatch_id: str) -> DispatchClaim:
        """Atomically classify and claim a dispatch_id for execution.

        Cloud Tasks delivers at-least-once, so a redelivery can arrive while
        the first delivery is still executing (issue #328). The two duplicate
        cases are *not* equivalent and must not share a response:

        - A completed dispatch has a durable terminal report behind it, so the
          redelivery can be acknowledged.
        - An in-flight dispatch has no outcome yet. Acknowledging it tells the
          queue the work succeeded while the executing request may still crash
          before reporting anything, stranding the attempt until the sweeper's
          much slower repair path notices. The redelivery is therefore told to
          retry, and mirrors whatever the owner ends up reporting.
        """
        with self._dedup_lock:
            if dispatch_id in self._recent_dispatch_ids:
                return DispatchClaim.COMPLETED
            if dispatch_id in self._inflight_dispatch_ids:
                return DispatchClaim.IN_FLIGHT
            self._inflight_dispatch_ids.add(dispatch_id)
            return DispatchClaim.CLAIMED

    def _release_dispatch(self, dispatch_id: str) -> None:
        """Release a dispatch_id's in-flight claim without recording completion.

        Recording into `_recent_dispatch_ids` happens separately, in
        `_record_dispatch_completed`, and only once the terminal report was
        delivered. If `_run_dispatch` raised before that point (for example a
        transient ApiError from the `task_started` callback), the dispatch_id
        must NOT enter the recent set: the worker returns HTTP 500, Cloud
        Tasks redelivers the same deterministic dispatch_id, and the
        redelivery has to re-execute rather than be duplicate-acked — else
        the task is stuck in Dispatched forever and the sweeper's repair
        dispatch (same dispatch_id) cannot rescue it.
        """
        with self._dedup_lock:
            self._inflight_dispatch_ids.discard(dispatch_id)

    def _record_dispatch_completed(self, dispatch_id: str) -> None:
        """Record a dispatch_id whose terminal report was delivered.

        Called from `_run_dispatch` immediately after `task_completed`
        returns (success or failure outcome alike). Only from this point on
        may a redelivery of the same dispatch_id be acknowledged without
        re-execution.
        """
        with self._dedup_lock:
            self._recent_dispatch_ids[dispatch_id] = None
            self._recent_dispatch_ids.move_to_end(dispatch_id)
            while len(self._recent_dispatch_ids) > RECENT_DISPATCH_LIMIT:
                self._recent_dispatch_ids.popitem(last=False)

    def _run_dispatch(self, payload: WorkerDispatchEnvelope) -> DispatchOutcome:
        task_token = _select_task_token(payload.task_token, self._fallback_task_token)
        started_at = _now_iso()
        try:
            self._client.task_started(
                task_id=payload.callback_task_id,
                task_key=payload.task_key,
                attempt=payload.attempt,
                attempt_id=payload.attempt_id,
                worker_id=self.worker_id,
                traceparent=payload.traceparent,
                started_at=started_at,
                task_token=task_token,
                callback_base_url=payload.callback_base_url,
            )
        except ApiError as err:
            if not _is_terminal_attempt_conflict(err):
                raise
            # The control plane holds a durable terminal result for this
            # attempt. This is the only signal that covers the crash window
            # between an accepted `task_completed` and this process recording
            # it — a replacement worker has no local memory of the attempt, so
            # the process-local set below is a fast path, never the guarantee.
            console.print(
                f"[blue]i[/blue] Attempt {payload.attempt} of {payload.task_key} is "
                f"already terminal in the control plane ({err}); skipping execution"
            )
            self._record_dispatch_completed(payload.dispatch_id)
            return DispatchOutcome.ALREADY_TERMINAL

        heartbeat = self._start_heartbeat(payload, task_token)

        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        output_payload: dict[str, Any] | None = None
        error_payload: dict[str, Any] | None = None
        outcome = "SUCCEEDED"

        try:
            with (
                contextlib.redirect_stdout(stdout_buffer),
                contextlib.redirect_stderr(stderr_buffer),
            ):
                result = self._execute_asset(payload)
            if isinstance(result, AssetOut):
                output_payload = {
                    "rowCount": result.row_count,
                }
        except Exception as exc:  # noqa: BLE001
            outcome = "FAILED"
            error_payload = {
                "category": "USER_CODE",
                "message": str(exc),
                "stackTrace": traceback.format_exc(),
            }
        finally:
            heartbeat.stop()
            completed_at = _now_iso()
            self._client.task_completed(
                task_id=payload.callback_task_id,
                task_key=payload.task_key,
                attempt=payload.attempt,
                attempt_id=payload.attempt_id,
                worker_id=self.worker_id,
                traceparent=payload.traceparent,
                outcome=outcome,
                completed_at=completed_at,
                output=output_payload,
                error=error_payload,
                task_token=task_token,
                callback_base_url=payload.callback_base_url,
            )
            # Only now — after the terminal report reached the control plane —
            # is it safe to duplicate-ack redeliveries of this dispatch_id.
            self._record_dispatch_completed(payload.dispatch_id)

            try:
                self._client.upload_logs(
                    workspace_id=self.config.workspace_id,
                    run_id=payload.run_id,
                    task_key=payload.task_key,
                    attempt=payload.attempt,
                    stdout=stdout_buffer.getvalue(),
                    stderr=stderr_buffer.getvalue(),
                )
            except ApiError as err:
                err_console.print(f"[yellow]![/yellow] Log upload failed: {err}")

        return DispatchOutcome.EXECUTED

    def _start_heartbeat(
        self,
        payload: WorkerDispatchEnvelope,
        task_token: str,
    ) -> HeartbeatSender:
        """Start periodic heartbeats for the attempt (issue #367).

        Anti-entropy force-fails a RUNNING task once it has been silent for
        `heartbeat_timeout_sec` plus grace, so long-running assets must
        heartbeat well inside that window. Heartbeat failures never fail the
        task.
        """

        def post_heartbeat() -> None:
            self._client.task_heartbeat(
                task_id=payload.callback_task_id,
                task_key=payload.task_key,
                attempt=payload.attempt,
                attempt_id=payload.attempt_id,
                worker_id=self.worker_id,
                traceparent=payload.traceparent,
                task_token=task_token,
                callback_base_url=payload.callback_base_url,
                heartbeat_at=_now_iso(),
            )

        heartbeat = HeartbeatSender(
            post=post_heartbeat,
            interval_seconds=heartbeat_interval_seconds(payload.heartbeat_timeout_sec),
        )
        heartbeat.start()
        return heartbeat

    def _execute_asset(self, payload: WorkerDispatchEnvelope) -> object:
        asset_func = self._assets.get(payload.task_key)
        if asset_func is None:
            msg = f"asset not found: {payload.task_key}"
            raise RuntimeError(msg)

        import inspect  # noqa: PLC0415

        signature = inspect.signature(asset_func)
        if len(signature.parameters) > 1:
            msg = "assets with dependencies are not supported by the minimal worker"
            raise RuntimeError(msg)

        ctx = AssetContext(
            partition_key=self._resolve_partition_key(payload),
            run_id=payload.run_id,
            task_id=payload.task_key,
            tenant_id=self.config.tenant_id,
            workspace_id=self.config.workspace_id,
        )

        result = asset_func(ctx)
        if asyncio.iscoroutine(result):
            result = asyncio.run(result)

        return result

    def _resolve_partition_key(self, payload: WorkerDispatchEnvelope) -> PartitionKey:
        """Build the execution partition scope from the dispatch envelope.

        The envelope's `partition_key` is the exact value the control plane
        records as materialized, so executing with anything else silently
        corrupts materialization identity (issue #339). A partitioned asset
        dispatched without a partition key fails loudly instead of executing
        unpartitioned while the catalog records a partition as materialized.

        Raises:
            RuntimeError: If a partitioned asset was dispatched without a
                partition key, or the partition key cannot be parsed.
        """
        if payload.partition_key is None:
            if payload.task_key in self._partitioned_assets:
                msg = (
                    f"asset {payload.task_key} is partitioned but the dispatch "
                    "envelope carried no partition key; refusing to execute "
                    "unpartitioned"
                )
                raise RuntimeError(msg)
            return PartitionKey()

        try:
            return PartitionKey.from_canonical_string(payload.partition_key)
        except ValueError as err:
            msg = f"invalid partition key in dispatch envelope: {payload.partition_key!r}: {err}"
            raise RuntimeError(msg) from err

    def _validate_dispatch_envelope(self, payload: WorkerDispatchEnvelope) -> None:
        mismatches = []
        if payload.tenant_id != self.config.tenant_id:
            mismatches.append("tenant_id")
        if payload.workspace_id != self.config.workspace_id:
            mismatches.append("workspace_id")
        configured_callback_base_url = str(getattr(self.config, "api_url", "") or "")
        if configured_callback_base_url and _normalize_base_url(
            payload.callback_base_url
        ) != _normalize_base_url(configured_callback_base_url):
            mismatches.append("callback_base_url")
        if mismatches:
            msg = "dispatch envelope scope mismatch: " + ", ".join(mismatches)
            raise ValueError(msg)


class DispatchHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        worker: DispatchWorker,
    ) -> None:  # noqa: D401
        self.worker = worker
        super().__init__(server_address, handler)


class DispatchHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/dispatch":
            self.send_response(404)
            self.end_headers()
            return

        if not _dispatch_authorized(
            self.server.worker.config,  # type: ignore[attr-defined]
            self.headers.get(DISPATCH_SECRET_HEADER),
            self.headers.get("Authorization"),
        ):
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b"missing or invalid dispatch authorization")
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
            dispatch = WorkerDispatchEnvelope.from_dict(payload)
        except Exception as exc:  # noqa: BLE001
            self.send_response(400)
            self.end_headers()
            self.wfile.write(str(exc).encode("utf-8"))
            return

        try:
            outcome = self.server.worker.handle_dispatch(dispatch)  # type: ignore[attr-defined]
        except ValueError as exc:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(str(exc).encode("utf-8"))
            return
        except Exception as exc:  # noqa: BLE001
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(exc).encode("utf-8"))
            return

        if outcome is DispatchOutcome.RETRY_LATER:
            # The owner of this dispatch id is still executing. A 200 here
            # would tell the queue the work succeeded before anyone knows
            # whether it did (issue #328); 503 keeps the delivery retryable so
            # a crashed owner is covered by the queue rather than by the much
            # slower anti-entropy repair.
            self.send_response(503)
            self.send_header("Retry-After", "1")
            self.end_headers()
            self.wfile.write(b"dispatch is still executing; retry")
            return

        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


def run_worker(
    *,
    host: str,
    port: int,
    root_path: Path | None,
    worker_id: str | None,
) -> None:
    """Start the worker HTTP server."""
    config = get_config()
    if not config.tenant_id:
        err_console.print("[red]✗[/red] Tenant ID not configured. Set ARCO_FLOW_TENANT_ID.")
        raise SystemExit(1)
    if not config.workspace_id:
        err_console.print("[red]✗[/red] Workspace ID not configured. Set ARCO_FLOW_WORKSPACE_ID.")
        raise SystemExit(1)
    if not config.worker_dispatch_secret.get_secret_value():
        err_console.print(
            "[red]✗[/red] Worker dispatch secret not configured. "
            "Set ARCO_FLOW_WORKER_DISPATCH_SECRET."
        )
        raise SystemExit(1)
    root = root_path or Path.cwd()

    console.print(f"[blue]i[/blue] Loading assets from {root}...")
    worker = DispatchWorker(config, root_path=root, worker_id=worker_id)

    server = DispatchHTTPServer((host, port), DispatchHandler, worker)
    console.print(f"[green]✓[/green] Worker listening on http://{host}:{port}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        console.print("\n[yellow]![/yellow] Shutting down worker.")
        server.shutdown()
    finally:
        worker.close()
