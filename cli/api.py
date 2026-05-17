from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Mapping


class ApiError(RuntimeError):
    """Raised when the backend API request fails."""

    def __init__(self, message: str, *, status: int | None = None, body: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.body = body


@dataclass(frozen=True)
class ApiBytesResponse:
    body: bytes
    headers: Mapping[str, str]
    status: int


def _param_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _clean_params(params: Mapping[str, Any] | None) -> dict[str, str]:
    if not params:
        return {}
    return {str(key): _param_value(value) for key, value in params.items() if value is not None}


class ApiClient:
    """Small stdlib HTTP client for backend API workflows."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        observer: Callable[[str, Mapping[str, Any]], None] | None = None,
    ) -> None:
        normalized = str(base_url or "").strip().rstrip("/")
        if not normalized:
            raise ValueError("base_url is required")
        self.base_url = normalized
        self.timeout = float(timeout)
        self.observer = observer

    def url(self, path: str, *, params: Mapping[str, Any] | None = None) -> str:
        normalized_path = "/" + str(path).lstrip("/")
        url = f"{self.base_url}{normalized_path}"
        query = urllib.parse.urlencode(_clean_params(params))
        return f"{url}?{query}" if query else url

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> Any:
        response = self.request_bytes(method, path, params=params, payload=payload)
        body = response.body.decode("utf-8")
        if not body:
            return {}
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise ApiError(
                f"{method.upper()} {self.url(path, params=params)} returned non-JSON response",
                status=response.status,
                body=body,
            ) from exc

    def request_bytes(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> ApiBytesResponse:
        body = json.dumps(payload or {}).encode("utf-8") if payload is not None else None
        url = self.url(path, params=params)
        started = time.monotonic()
        request_bytes = len(body or b"")
        request = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method=method.upper(),
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                headers = {str(key): str(value) for key, value in response.headers.items()}
                status = int(getattr(response, "status", 200))
                response_body = response.read()
                if self.observer:
                    self.observer(
                        "http_request",
                        {
                            "method": method.upper(),
                            "url": url,
                            "status": status,
                            "duration_ms": round((time.monotonic() - started) * 1000, 3),
                            "request_bytes": request_bytes,
                            "response_bytes": len(response_body),
                        },
                    )
                return ApiBytesResponse(body=response_body, headers=headers, status=status)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            if self.observer:
                self.observer(
                    "http_request",
                    {
                        "method": method.upper(),
                        "url": url,
                        "status": int(exc.code),
                        "duration_ms": round((time.monotonic() - started) * 1000, 3),
                        "request_bytes": request_bytes,
                        "response_bytes": len(detail.encode("utf-8")),
                        "error": "http_error",
                    },
                )
            raise ApiError(
                f"{method.upper()} {url} failed with status {exc.code}",
                status=int(exc.code),
                body=detail,
            ) from exc
        except urllib.error.URLError as exc:
            if self.observer:
                self.observer(
                    "http_request",
                    {
                        "method": method.upper(),
                        "url": url,
                        "duration_ms": round((time.monotonic() - started) * 1000, 3),
                        "request_bytes": request_bytes,
                        "error": str(exc.reason),
                    },
                )
            raise ApiError(f"{method.upper()} {url} failed: {exc.reason}") from exc


def filename_from_content_disposition(value: str | None, fallback: str) -> str:
    if not value:
        return fallback
    for part in str(value).split(";"):
        item = part.strip()
        if item.lower().startswith("filename="):
            return item.split("=", 1)[1].strip().strip('"') or fallback
    return fallback
