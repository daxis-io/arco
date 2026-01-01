"""Minimal worker HTTP server for executing dispatched tasks."""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import socket
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import Any

from rich.console import Console

from arco_flow.cli.config import ArcoFlowConfig, get_config
from arco_flow.client import ApiError, ArcoFlowApiClient
from arco_flow.context import AssetContext
from arco_flow.manifest.discovery import AssetDiscovery, AssetDiscoveryError
from arco_flow.types import AssetOut, PartitionKey

console = Console()
err_console = Console(stderr=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> DispatchPayload:
        run_id = _get_field(payload, "run_id", "runId")
        task_key = _get_field(payload, "task_key", "taskKey")
        attempt = _get_field(payload, "attempt", "attempt")
        attempt_id = _get_field(payload, "attempt_id", "attemptId")
        traceparent = _get_field(payload, "traceparent", "traceparent")

        if not run_id or not task_key or not attempt or not attempt_id:
            msg = "dispatch payload missing required fields"
            raise ValueError(msg)

        return cls(
            run_id=str(run_id),
            task_key=str(task_key),
            attempt=int(attempt),
            attempt_id=str(attempt_id),
            traceparent=str(traceparent) if traceparent else None,
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
        self._task_token = (
            config.task_token.get_secret_value() or config.api_key.get_secret_value() or "debug"
        )
        self._client = ArcoFlowApiClient(config)
        self._assets = self._load_assets(root_path)

    def _load_assets(self, root_path: Path) -> dict[str, Any]:
        discovery = AssetDiscovery(root_path=root_path)
        try:
            assets = discovery.discover(strict=True)
        except AssetDiscoveryError as err:
            err_console.print("[red]✗[/red] Asset discovery failed")
            for failure in err.failures:
                err_console.print(f"  - {failure.file_path}: {failure.error}")
            raise SystemExit(1) from err

        return {str(asset.key): asset.func for asset in assets}

    def close(self) -> None:
        """Close worker resources."""
        self._client.close()

    def handle_dispatch(self, payload: DispatchPayload) -> None:
        started_at = _now_iso()
        self._client.task_started(
            task_key=payload.task_key,
            attempt=payload.attempt,
            attempt_id=payload.attempt_id,
            worker_id=self.worker_id,
            traceparent=payload.traceparent,
            started_at=started_at,
            task_token=self._task_token,
        )

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
            completed_at = _now_iso()
            self._client.task_completed(
                task_key=payload.task_key,
                attempt=payload.attempt,
                attempt_id=payload.attempt_id,
                worker_id=self.worker_id,
                traceparent=payload.traceparent,
                outcome=outcome,
                completed_at=completed_at,
                output=output_payload,
                error=error_payload,
                task_token=self._task_token,
            )

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

    def _execute_asset(self, payload: DispatchPayload) -> object:
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
            partition_key=PartitionKey(),
            run_id=payload.run_id,
            task_id=payload.task_key,
            tenant_id=self.config.tenant_id,
            workspace_id=self.config.workspace_id,
        )

        result = asset_func(ctx)
        if asyncio.iscoroutine(result):
            result = asyncio.run(result)

        return result


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

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
            dispatch = DispatchPayload.from_dict(payload)
        except Exception as exc:  # noqa: BLE001
            self.send_response(400)
            self.end_headers()
            self.wfile.write(str(exc).encode("utf-8"))
            return

        try:
            self.server.worker.handle_dispatch(dispatch)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(exc).encode("utf-8"))
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
    if not config.debug and not (
        config.api_key.get_secret_value() or config.task_token.get_secret_value()
    ):
        err_console.print(
            "[red]✗[/red] API key or task token required. Set ARCO_FLOW_API_KEY or ARCO_FLOW_TASK_TOKEN."
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
