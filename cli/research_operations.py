"""Shared thin adapter for the canonical research-evidence application surface."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .api import ApiClient


class ResearchOperations:
    """Expose one payload contract to CLI and MCP without rebuilding semantics."""

    def __init__(self, client: ApiClient) -> None:
        self._client = client

    @staticmethod
    def _payload(value: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise ValueError("research operation request must be an object")
        return dict(value)

    def definitions(self) -> dict[str, Any]:
        return self._client.request_json("GET", "/api/research/checks/definitions")

    def definition(self, definition_id: str, version: str) -> dict[str, Any]:
        from urllib.parse import quote

        return self._client.request_json(
            "GET", f"/api/research/checks/definitions/{quote(definition_id, safe='')}/{quote(version, safe='')}"
        )

    def requirements(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._client.request_json(
            "POST",
            "/api/research/checks/requirements",
            payload=self._payload(request),
        )

    def preview(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._client.request_json(
            "POST",
            "/api/research/checks/evaluate",
            payload={**self._payload(request), "mode": "preview"},
        )

    def prepare(
        self,
        request: Mapping[str, Any],
        *,
        freeze: bool,
        created_by: str | None = None,
        dataset_name: str | None = None,
    ) -> dict[str, Any]:
        payload = self._payload(request)
        preparation = dict(payload.get("preparation") or {})
        preparation["freeze"] = bool(freeze)
        if created_by:
            preparation["created_by"] = str(created_by)
        if dataset_name:
            preparation["name"] = str(dataset_name)
        return self._client.request_json(
            "POST",
            "/api/research/checks/prepare",
            payload={**payload, "preparation": preparation},
        )

    def run_evidence(
        self,
        request: Mapping[str, Any],
        *,
        dataset_id: str | None = None,
    ) -> dict[str, Any]:
        payload = {**self._payload(request), "mode": "evidence"}
        if dataset_id:
            payload["dataset_id"] = str(dataset_id)
        return self._client.request_json(
            "POST", "/api/research/checks/run", payload=payload
        )

    def dispatch_evidence(
        self,
        request: Mapping[str, Any],
        *,
        dataset_id: str | None = None,
        single_attempt: bool = False,
    ) -> dict[str, Any]:
        payload = {**self._payload(request), "mode": "evidence"}
        if dataset_id:
            payload["dataset_id"] = str(dataset_id)
        return self._client.request_json(
            "POST",
            "/api/research/jobs/checks/run-once" if single_attempt else "/api/research/jobs/checks/run",
            payload=payload
        )

    def job_status(self, job_id: str) -> dict[str, Any]:
        normalized = str(job_id or "").strip()
        if not normalized:
            raise ValueError("job_id is required")
        return self._client.request_json(
            "GET", f"/api/research/jobs/{normalized}"
        )

    def job_result(self, job_id: str) -> dict[str, Any]:
        normalized = str(job_id or "").strip()
        if not normalized:
            raise ValueError("job_id is required")
        return self._client.request_json(
            "GET", f"/api/research/jobs/{normalized}/result"
        )

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        normalized = str(job_id or "").strip()
        if not normalized:
            raise ValueError("job_id is required")
        return self._client.request_json("POST", f"/api/research/jobs/{normalized}/cancel")

    def evaluate_pass_gates(
        self, request: Mapping[str, Any]
    ) -> dict[str, Any]:
        return self._client.request_json(
            "POST",
            "/api/research/comparisons/pass-gates/evaluate",
            payload=self._payload(request),
        )

    def replay(self, check_id: str) -> dict[str, Any]:
        normalized = str(check_id or "").strip()
        if not normalized:
            raise ValueError("check_id is required")
        return self._client.request_json(
            "POST", f"/api/research/checks/{normalized}/replay", payload={}
        )

    def create_observation(
        self, check_id: str, request: Mapping[str, Any]
    ) -> dict[str, Any]:
        normalized = str(check_id or "").strip()
        if not normalized:
            raise ValueError("check_id is required")
        return self._client.request_json(
            "POST",
            f"/api/research/checks/{normalized}/observations",
            payload=self._payload(request),
        )

    def trail(self, item_id: str) -> dict[str, Any]:
        normalized = str(item_id or "").strip()
        if not normalized:
            raise ValueError("item_id is required")
        return self._client.request_json(
            "GET", f"/api/research/items/{normalized}/trail"
        )

    def adopt_question(self, item_id: str, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._client.request_json("POST", f"/api/research/items/{item_id}/question", payload=self._payload(request))

    def publish_interpretation(self, item_id: str, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._client.request_json("POST", f"/api/research/items/{item_id}/publications", payload=self._payload(request))

    def publication_history(self, item_id: str) -> dict[str, Any]:
        return self._client.request_json("GET", f"/api/research/items/{item_id}/publications")


__all__ = ["ResearchOperations"]
