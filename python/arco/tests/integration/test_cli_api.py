"""CLI integration tests for API wiring."""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from textwrap import dedent
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from typer.testing import CliRunner

from servo.cli.config import clear_config_cache
from servo.cli.main import app


@dataclass
class RecordedRequest:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


class _RecordingHandler(BaseHTTPRequestHandler):
    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return b""
        return self.rfile.read(length)

    def _record(self, body: bytes) -> None:
        record = RecordedRequest(
            method=self.command,
            path=self.path,
            headers={key: value for key, value in self.headers.items()},
            body=body,
        )
        self.server.records.append(record)  # type: ignore[attr-defined]

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, status: int, text: str) -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        body = self._read_body()
        self._record(body)
        path = urlparse(self.path).path

        if path.endswith("/manifests"):
            self._send_json(
                201,
                {
                    "manifestId": "manifest-01",
                    "workspaceId": "test-workspace",
                    "codeVersionId": "abc123",
                    "fingerprint": "fp-123",
                    "assetCount": 2,
                    "deployedAt": "2025-01-01T00:00:00Z",
                },
            )
            return

        if path.endswith("/runs"):
            run_id = self.server.run_id  # type: ignore[attr-defined]
            self._send_json(
                201,
                {
                    "runId": run_id,
                    "planId": "plan-01",
                    "state": "PENDING",
                    "created": True,
                    "createdAt": "2025-01-01T00:00:00Z",
                },
            )
            return

        self._send_json(404, {"error": "not_found"})

    def do_GET(self) -> None:  # noqa: N802
        body = self._read_body()
        self._record(body)
        parsed = urlparse(self.path)
        path = parsed.path

        if path.endswith("/logs"):
            self._send_text(200, "=== stdout ===\nhello\n=== stderr ===\n")
            return

        if "/runs/" in path:
            run_id = path.rsplit("/", 1)[-1]
            self._send_json(
                200,
                {
                    "runId": run_id,
                    "state": "SUCCEEDED",
                    "tasks": [],
                },
            )
            return

        if path.endswith("/runs"):
            self._send_json(
                200,
                {
                    "runs": [
                        {
                            "runId": self.server.run_id,  # type: ignore[attr-defined]
                            "state": "SUCCEEDED",
                            "createdAt": "2025-01-01T00:00:00Z",
                            "taskCount": 1,
                            "tasksFailed": 0,
                        }
                    ]
                },
            )
            return

        self._send_json(404, {"error": "not_found"})

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


@pytest.fixture()
def api_server() -> dict[str, Any]:
    records: list[RecordedRequest] = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RecordingHandler)
    server.records = records  # type: ignore[attr-defined]
    server.run_id = "01HXTESTRUN"  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    host, port = server.server_address
    base_url = f"http://{host}:{port}"
    try:
        yield {"base_url": base_url, "records": records, "run_id": server.run_id}
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture()
def servo_env(monkeypatch: pytest.MonkeyPatch, api_server: dict[str, Any]) -> None:
    monkeypatch.setenv("SERVO_API_URL", api_server["base_url"])
    monkeypatch.setenv("SERVO_TENANT_ID", "test-tenant")
    monkeypatch.setenv("SERVO_WORKSPACE_ID", "test-workspace")
    monkeypatch.setenv("SERVO_DEBUG", "1")
    clear_config_cache()
    yield None
    clear_config_cache()


@pytest.fixture
def project_with_assets(tmp_path: Path) -> Path:
    """Create a project with multiple assets."""
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "__init__.py").write_text("")

    (tmp_path / "assets" / "raw.py").write_text(dedent("""
        from servo import asset
        from servo.context import AssetContext
        from servo.types import AssetOut, DailyPartition

        @asset(namespace="raw", partitions=DailyPartition("date"))
        def events(ctx: AssetContext) -> AssetOut:
            '''Raw event data.'''
            return ctx.output([])
    """))

    (tmp_path / "assets" / "staging.py").write_text(dedent("""
        from servo import asset
        from servo.context import AssetContext
        from servo.types import AssetIn, AssetOut, DailyPartition, row_count

        @asset(
            namespace="staging",
            partitions=DailyPartition("date"),
            checks=[row_count(min_rows=1)],
        )
        def cleaned_events(ctx: AssetContext, raw: AssetIn["raw.events"]) -> AssetOut:
            '''Cleaned events.'''
            return ctx.output([])
    """))

    return tmp_path


def _find_request(
    records: list[RecordedRequest],
    *,
    method: str,
    path_prefix: str,
) -> RecordedRequest:
    for record in records:
        if record.method == method and record.path.startswith(path_prefix):
            return record
    msg = f"missing {method} request for {path_prefix}"
    raise AssertionError(msg)


def test_cli_run_status_logs(api_server: dict[str, Any], servo_env: None) -> None:
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "run",
            "raw.events",
            "--partition",
            "date=2025-01-15",
            "--run-key",
            "rk-001",
        ],
    )
    assert result.exit_code == 0

    run_request = _find_request(
        api_server["records"],
        method="POST",
        path_prefix="/api/v1/workspaces/test-workspace/runs",
    )
    payload = json.loads(run_request.body)
    assert payload["selection"] == ["raw.events"]
    assert payload["partitions"] == [{"key": "date", "value": "2025-01-15"}]
    assert payload["runKey"] == "rk-001"
    assert run_request.headers.get("X-Tenant-Id") == "test-tenant"
    assert run_request.headers.get("X-Workspace-Id") == "test-workspace"

    run_id = api_server["run_id"]
    result = runner.invoke(app, ["status", run_id])
    assert result.exit_code == 0
    _find_request(
        api_server["records"],
        method="GET",
        path_prefix=f"/api/v1/workspaces/test-workspace/runs/{run_id}",
    )

    result = runner.invoke(app, ["logs", run_id, "--task", "raw.events"])
    assert result.exit_code == 0
    logs_request = _find_request(
        api_server["records"],
        method="GET",
        path_prefix=f"/api/v1/workspaces/test-workspace/runs/{run_id}/logs",
    )
    parsed = urlparse(logs_request.path)
    assert parse_qs(parsed.query).get("taskKey") == ["raw.events"]


def test_cli_deploy_hits_api(
    api_server: dict[str, Any],
    servo_env: None,
    project_with_assets: Path,
) -> None:
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=project_with_assets):
        result = runner.invoke(app, ["deploy"])
    assert result.exit_code == 0

    deploy_request = _find_request(
        api_server["records"],
        method="POST",
        path_prefix="/api/v1/workspaces/test-workspace/manifests",
    )
    payload = json.loads(deploy_request.body)
    assert payload["tenantId"] == "test-tenant"
    assert payload["workspaceId"] == "test-workspace"
    assert payload["assets"]
    assert deploy_request.headers.get("Idempotency-Key")
