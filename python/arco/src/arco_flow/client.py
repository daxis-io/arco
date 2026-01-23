"""HTTP client for the Arco Flow control plane API."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

import httpx

from arco_flow import __version__
from arco_flow.cli.config import ArcoFlowConfig


class ApiError(RuntimeError):
    """HTTP API error."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass
class ApiResponse:
    """Lightweight wrapper for API responses."""

    payload: dict[str, Any]


class ArcoFlowApiClient:
    """HTTP client for Arco Flow API endpoints."""

    def __init__(self, config: ArcoFlowConfig, *, timeout: float = 30.0) -> None:
        self._config = config
        self._base_url = config.api_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> ArcoFlowApiClient:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:  # noqa: ANN001
        self.close()

    def _build_headers(
        self,
        *,
        idempotency_key: str | None = None,
        task_token: str | None = None,
        tenant_id: str | None = None,
        workspace_id: str | None = None,
        user_id: str | None = None,
    ) -> dict[str, str]:
        headers: dict[str, str] = {
            "User-Agent": f"arco-flow-cli/{__version__}",
        }

        if self._config.debug:
            resolved_tenant = self._config.tenant_id if tenant_id is None else tenant_id
            resolved_workspace = self._config.workspace_id if workspace_id is None else workspace_id
            resolved_user = self._config.user_id if user_id is None else user_id
            if resolved_tenant:
                headers["X-Tenant-Id"] = resolved_tenant
            if resolved_workspace:
                headers["X-Workspace-Id"] = resolved_workspace
            if resolved_user:
                headers["X-User-Id"] = resolved_user

        token = task_token or self._config.api_key.get_secret_value()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key

        return headers

    def _url(self, path: str) -> str:
        return f"{self._base_url}/api/v1{path}"

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        response = self._client.request(
            method,
            self._url(path),
            json=json_body,
            params=params,
            headers=headers,
        )
        self._raise_for_status(response)
        return response.json()

    def _request_text(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        response = self._client.request(
            method,
            self._url(path),
            params=params,
            headers=headers,
        )
        self._raise_for_status(response)
        return response.text

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.is_success:
            return
        message = response.text
        try:
            data = response.json()
            message = data.get("message") or data.get("error") or data.get("detail") or message
        except json.JSONDecodeError:
            pass
        raise ApiError(response.status_code, f"{response.status_code}: {message}")

    def deploy_manifest(
        self,
        *,
        workspace_id: str,
        manifest_json: str,
        idempotency_key: str | None = None,
    ) -> ApiResponse:
        payload = json.loads(manifest_json)
        headers = self._build_headers(
            idempotency_key=idempotency_key,
            workspace_id=workspace_id,
        )
        response = self._request_json(
            "POST",
            f"/workspaces/{workspace_id}/manifests",
            json_body=payload,
            headers=headers,
        )
        return ApiResponse(payload=response)

    def trigger_run(
        self,
        *,
        workspace_id: str,
        selection: list[str],
        partitions: list[dict[str, str]] | None = None,
        partition_key: str | None = None,
        run_key: str | None = None,
    ) -> ApiResponse:
        headers = self._build_headers(workspace_id=workspace_id)

        if partitions is None:
            partitions = []

        if partition_key == "":
            raise ValueError("partition_key cannot be empty")

        if partition_key and partitions:
            raise ValueError("use either partition_key or partitions, not both")

        payload: dict[str, Any] = {
            "selection": selection,
        }
        if partition_key:
            payload["partitionKey"] = partition_key
        elif partitions:
            payload["partitions"] = partitions

        if run_key:
            payload["runKey"] = run_key

        response = self._request_json(
            "POST",
            f"/workspaces/{workspace_id}/runs",
            json_body=payload,
            headers=headers,
        )
        return ApiResponse(payload=response)

    def list_runs(
        self,
        *,
        workspace_id: str,
        limit: int,
    ) -> ApiResponse:
        headers = self._build_headers(workspace_id=workspace_id)
        response = self._request_json(
            "GET",
            f"/workspaces/{workspace_id}/runs",
            params={"limit": limit},
            headers=headers,
        )
        return ApiResponse(payload=response)

    def get_run(self, *, workspace_id: str, run_id: str) -> ApiResponse:
        headers = self._build_headers(workspace_id=workspace_id)
        response = self._request_json(
            "GET",
            f"/workspaces/{workspace_id}/runs/{run_id}",
            headers=headers,
        )
        return ApiResponse(payload=response)

    def rerun_run(
        self,
        *,
        workspace_id: str,
        run_id: str,
        mode: str,  # "fromFailure" or "subset"
        selection: list[str] | None = None,
        include_upstream: bool = False,
        include_downstream: bool = False,
        run_key: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> ApiResponse:
        headers = self._build_headers(workspace_id=workspace_id)

        if selection is None:
            selection = []

        payload: dict[str, Any] = {
            "mode": mode,
            "selection": selection,
        }

        if include_upstream:
            payload["includeUpstream"] = True
        if include_downstream:
            payload["includeDownstream"] = True

        if run_key:
            payload["runKey"] = run_key
        if labels:
            payload["labels"] = labels

        response = self._request_json(
            "POST",
            f"/workspaces/{workspace_id}/runs/{run_id}/rerun",
            json_body=payload,
            headers=headers,
        )
        return ApiResponse(payload=response)

    def upload_logs(
        self,
        *,
        workspace_id: str,
        run_id: str,
        task_key: str,
        attempt: int,
        stdout: str,
        stderr: str,
    ) -> ApiResponse:
        headers = self._build_headers(workspace_id=workspace_id)
        payload = {
            "taskKey": task_key,
            "attempt": attempt,
            "stdout": stdout,
            "stderr": stderr,
        }
        response = self._request_json(
            "POST",
            f"/workspaces/{workspace_id}/runs/{run_id}/logs",
            json_body=payload,
            headers=headers,
        )
        return ApiResponse(payload=response)

    def get_logs(
        self,
        *,
        workspace_id: str,
        run_id: str,
        task_key: str | None = None,
    ) -> str:
        headers = self._build_headers(workspace_id=workspace_id)
        params = {"taskKey": task_key} if task_key else None
        return self._request_text(
            "GET",
            f"/workspaces/{workspace_id}/runs/{run_id}/logs",
            params=params,
            headers=headers,
        )

    def task_started(
        self,
        *,
        task_key: str,
        attempt: int,
        attempt_id: str,
        worker_id: str,
        traceparent: str | None,
        started_at: str,
        task_token: str,
    ) -> ApiResponse:
        headers = self._build_headers(task_token=task_token)
        payload = {
            "attempt": attempt,
            "attemptId": attempt_id,
            "workerId": worker_id,
            "traceparent": traceparent,
            "startedAt": started_at,
        }
        response = self._request_json(
            "POST",
            f"/tasks/{task_key}/started",
            json_body=payload,
            headers=headers,
        )
        return ApiResponse(payload=response)

    def task_completed(
        self,
        *,
        task_key: str,
        attempt: int,
        attempt_id: str,
        worker_id: str,
        traceparent: str | None,
        outcome: str,
        completed_at: str,
        output: dict[str, Any] | None,
        error: dict[str, Any] | None,
        task_token: str,
    ) -> ApiResponse:
        headers = self._build_headers(task_token=task_token)
        payload: dict[str, Any] = {
            "attempt": attempt,
            "attemptId": attempt_id,
            "workerId": worker_id,
            "traceparent": traceparent,
            "outcome": outcome,
            "completedAt": completed_at,
        }
        if output is not None:
            payload["output"] = output
        if error is not None:
            payload["error"] = error
        response = self._request_json(
            "POST",
            f"/tasks/{task_key}/completed",
            json_body=payload,
            headers=headers,
        )
        return ApiResponse(payload=response)
