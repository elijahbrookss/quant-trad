from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import re
import shlex
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Mapping


DEFAULT_LOKI_URL = "http://localhost:3100"
DEFAULT_RUN_SELECTORS = ('{service="bot-runtime"}', '{service="backend"}', '{service="docker-events"}')
DEFAULT_LOOKBACK_HOURS = 6.0

_LOG_HEADER = re.compile(
    r"^(?P<wall_time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+"
    r"(?P<level>[A-Z]+)\s+"
    r"(?P<source>[^|]+?)\s+\|\s+"
    r"(?P<body>.*)$"
)
_KV_TOKEN = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)=(?P<value>\S+)")


@dataclass(frozen=True)
class LokiEntry:
    timestamp_ns: str
    labels: Mapping[str, str]
    line: str
    parsed: Mapping[str, Any]

    def as_payload(self) -> dict[str, Any]:
        return {
            "timestamp_ns": self.timestamp_ns,
            "timestamp": _ns_to_iso(self.timestamp_ns),
            "labels": dict(self.labels),
            "parsed": dict(self.parsed),
            "line": self.line,
        }


class LokiClient:
    def __init__(self, base_url: str = DEFAULT_LOKI_URL, *, timeout: float = 10.0) -> None:
        normalized = str(base_url or "").strip().rstrip("/")
        if not normalized:
            raise ValueError("loki url is required")
        self.base_url = normalized
        self.timeout = float(timeout)

    def query_range(
        self,
        *,
        query: str,
        start: str,
        end: str,
        limit: int,
        direction: str = "forward",
    ) -> dict[str, Any]:
        params = urllib.parse.urlencode(
            {
                "query": query,
                "start": start,
                "end": end,
                "limit": int(limit),
                "direction": direction,
            }
        )
        url = f"{self.base_url}/loki/api/v1/query_range?{params}"
        request = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read().decode("utf-8")
        except urllib.error.URLError as exc:
            raise ValueError(
                f"Loki is unreachable at {self.base_url}; start the observability profile and retry"
            ) from exc
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Loki returned non-JSON response from {url}") from exc
        if payload.get("status") != "success":
            raise ValueError(f"Loki query failed: {payload}")
        return payload

    def ready(self) -> str:
        url = f"{self.base_url}/ready"
        request = urllib.request.Request(url, headers={"Accept": "text/plain"}, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.read().decode("utf-8").strip()
        except urllib.error.URLError as exc:
            raise ValueError(
                f"Loki is unreachable at {self.base_url}; start the observability profile and retry"
            ) from exc

    def label_values(self, label: str, *, start: str, end: str) -> list[str]:
        normalized_label = str(label or "").strip()
        if not normalized_label:
            raise ValueError("label is required")
        params = urllib.parse.urlencode({"start": start, "end": end})
        url = f"{self.base_url}/loki/api/v1/label/{urllib.parse.quote(normalized_label)}/values?{params}"
        request = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = response.read().decode("utf-8")
        except urllib.error.URLError as exc:
            raise ValueError(
                f"Loki is unreachable at {self.base_url}; start the observability profile and retry"
            ) from exc
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Loki returned non-JSON response from {url}") from exc
        if payload.get("status") != "success":
            raise ValueError(f"Loki label query failed: {payload}")
        values = payload.get("data") or []
        if not isinstance(values, list):
            raise ValueError(f"Loki label query returned unexpected data for {normalized_label}: {payload}")
        return sorted(str(value) for value in values)


def default_loki_url(raw: str | None = None) -> str:
    value = str(raw or "").strip()
    return value or DEFAULT_LOKI_URL


def default_window(*, lookback_hours: float = DEFAULT_LOOKBACK_HOURS) -> tuple[str, str]:
    end = datetime.now(UTC)
    start = end - timedelta(hours=float(lookback_hours))
    return _to_loki_time(start), _to_loki_time(end)


def normalize_window(
    *,
    start: str | None,
    end: str | None,
    lookback_hours: float = DEFAULT_LOOKBACK_HOURS,
) -> tuple[str, str]:
    default_start, default_end = default_window(lookback_hours=lookback_hours)
    return (_normalize_time(start) if start else default_start, _normalize_time(end) if end else default_end)


def run_log_payload(
    *,
    client: LokiClient,
    run_id: str,
    bot_id: str | None = None,
    start: str | None = None,
    end: str | None = None,
    lookback_hours: float = DEFAULT_LOOKBACK_HOURS,
    limit: int = 500,
    selectors: tuple[str, ...] = DEFAULT_RUN_SELECTORS,
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise ValueError("run_id is required")
    window_start, window_end = normalize_window(start=start, end=end, lookback_hours=lookback_hours)
    run_entries: list[LokiEntry] = []
    for selector in selectors:
        run_entries.extend(
            _query_entries(
                client,
                query=_line_filter(selector, normalized_run_id),
                start=window_start,
                end=window_end,
                limit=limit,
            )
        )
    entries: list[LokiEntry] = list(run_entries)
    context_bounds = _context_bounds(run_entries, padding_seconds=60.0)
    bot_ids = {str(bot_id).strip()} if bot_id else set()
    bot_ids.update(
        str(entry.parsed.get("fields", {}).get("bot_id") or "").strip()
        for entry in entries
        if str(entry.parsed.get("fields", {}).get("bot_id") or "").strip()
    )
    for candidate_bot_id in sorted(bot_ids):
        for selector in selectors:
            bot_entries = _query_entries(
                client,
                query=_line_filter(_line_filter(selector, candidate_bot_id), "docker_lifecycle_event"),
                start=window_start,
                end=window_end,
                limit=limit,
            )
            entries.extend(_entries_in_bounds(bot_entries, context_bounds))
    deduped = _dedupe_entries(entries)
    return {
        "schema_version": "qt_loki_run_logs.v1",
        "loki_url": client.base_url,
        "selectors": list(selectors),
        "run_id": normalized_run_id,
        "bot_ids": sorted(bot_ids),
        "query_window": {"start": window_start, "end": window_end},
        "summary": summarize_entries(deduped),
        "entries": [entry.as_payload() for entry in deduped],
    }


def doctor_log_payload(
    *,
    client: LokiClient,
    start: str | None = None,
    end: str | None = None,
    lookback_hours: float = 24.0,
) -> dict[str, Any]:
    window_start, window_end = normalize_window(start=start, end=end, lookback_hours=lookback_hours)
    ready = client.ready()
    jobs = client.label_values("job", start=window_start, end=window_end)
    services = client.label_values("service", start=window_start, end=window_end)
    runtimes = client.label_values("runtime", start=window_start, end=window_end)
    checks = [
        _doctor_check(
            "loki_ready",
            ok=ready.lower() == "ready",
            detail=ready or "empty readiness response",
        ),
        _doctor_check(
            "quanttrad_job_visible",
            ok="quanttrad" in jobs,
            detail="job=quanttrad found" if "quanttrad" in jobs else "no job=quanttrad streams in query window",
        ),
        _doctor_check(
            "backend_service_visible",
            ok="backend" in services,
            detail="service=backend found" if "backend" in services else "backend stream absent in query window",
        ),
        _doctor_check(
            "bot_runtime_service_visible",
            ok="bot-runtime" in services,
            detail=(
                "service=bot-runtime found"
                if "bot-runtime" in services
                else "no bot-runtime stream in query window; run a bot with observability active to prove runtime ingestion"
            ),
        ),
        _doctor_check(
            "bot_runtime_label_visible",
            ok="bot" in runtimes,
            detail=(
                "runtime=bot found"
                if "bot" in runtimes
                else "no runtime=bot label in query window; Promtail may need restart after config changes"
            ),
        ),
    ]
    status = "ok" if all(check["status"] == "ok" for check in checks) else "needs_attention"
    return {
        "schema_version": "qt_loki_doctor.v1",
        "loki_url": client.base_url,
        "status": status,
        "ingestion_contract": "docker_stdout_promtail_loki",
        "query_window": {"start": window_start, "end": window_end},
        "labels": {
            "jobs": jobs,
            "services": services,
            "runtimes": runtimes,
        },
        "checks": checks,
    }


def query_log_payload(
    *,
    client: LokiClient,
    logql: str,
    start: str | None = None,
    end: str | None = None,
    lookback_hours: float = DEFAULT_LOOKBACK_HOURS,
    limit: int = 500,
) -> dict[str, Any]:
    query = str(logql or "").strip()
    if not query:
        raise ValueError("logql query is required")
    window_start, window_end = normalize_window(start=start, end=end, lookback_hours=lookback_hours)
    entries = _query_entries(client, query=query, start=window_start, end=window_end, limit=limit)
    return {
        "schema_version": "qt_loki_query_logs.v1",
        "loki_url": client.base_url,
        "query": query,
        "query_window": {"start": window_start, "end": window_end},
        "summary": summarize_entries(entries),
        "entries": [entry.as_payload() for entry in entries],
    }


def summarize_entries(entries: list[LokiEntry]) -> dict[str, Any]:
    events: Counter[str] = Counter()
    levels: Counter[str] = Counter()
    services: Counter[str] = Counter()
    for entry in entries:
        parsed = entry.parsed
        event = str(parsed.get("event") or "").strip()
        level = str(parsed.get("level") or "").strip()
        service = str(entry.labels.get("service") or entry.labels.get("service_name") or "").strip()
        if event:
            events[event] += 1
        if level:
            levels[level] += 1
        if service:
            services[service] += 1
    return {
        "entries": len(entries),
        "events": dict(sorted(events.items())),
        "levels": dict(sorted(levels.items())),
        "services": dict(sorted(services.items())),
    }


def parse_log_line(line: str) -> dict[str, Any]:
    raw = str(line or "")
    match = _LOG_HEADER.match(raw)
    if not match:
        return {"message": raw, "fields": {}, "event": None}
    body = match.group("body").strip()
    parts = [part.strip() for part in body.split(" | ") if part.strip()]
    message = parts[0] if parts else ""
    fields: dict[str, str] = {}
    for segment in parts[1:]:
        fields.update(_parse_kv_segment(segment))
    if message.startswith("event="):
        fields.update(_parse_kv_segment(message))
    event = fields.get("event") or (message if _looks_like_event(message) else None)
    return {
        "wall_time": match.group("wall_time"),
        "level": match.group("level"),
        "source": match.group("source").strip(),
        "message": message,
        "event": event,
        "fields": fields,
    }


def _query_entries(
    client: LokiClient,
    *,
    query: str,
    start: str,
    end: str,
    limit: int,
) -> list[LokiEntry]:
    payload = client.query_range(query=query, start=start, end=end, limit=limit, direction="forward")
    entries: list[LokiEntry] = []
    for stream in payload.get("data", {}).get("result", []) or []:
        labels = {str(key): str(value) for key, value in dict(stream.get("stream") or {}).items()}
        for value in stream.get("values") or []:
            if not isinstance(value, list) or len(value) != 2:
                continue
            timestamp_ns, line = str(value[0]), str(value[1])
            entries.append(
                LokiEntry(
                    timestamp_ns=timestamp_ns,
                    labels=labels,
                    line=line,
                    parsed=parse_log_line(line),
                )
            )
    return sorted(entries, key=lambda item: int(item.timestamp_ns))


def _dedupe_entries(entries: list[LokiEntry]) -> list[LokiEntry]:
    seen: set[tuple[str, str, tuple[tuple[str, str], ...]]] = set()
    deduped: list[LokiEntry] = []
    for entry in sorted(entries, key=lambda item: int(item.timestamp_ns)):
        identity = (entry.timestamp_ns, entry.line, tuple(sorted(entry.labels.items())))
        if identity in seen:
            continue
        seen.add(identity)
        deduped.append(entry)
    return deduped


def _context_bounds(entries: list[LokiEntry], *, padding_seconds: float) -> tuple[int, int] | None:
    if not entries:
        return None
    timestamps = [int(entry.timestamp_ns) for entry in entries]
    padding_ns = int(float(padding_seconds) * 1_000_000_000)
    return min(timestamps) - padding_ns, max(timestamps) + padding_ns


def _entries_in_bounds(entries: list[LokiEntry], bounds: tuple[int, int] | None) -> list[LokiEntry]:
    if bounds is None:
        return entries
    start, end = bounds
    return [entry for entry in entries if start <= int(entry.timestamp_ns) <= end]


def _doctor_check(name: str, *, ok: bool, detail: str) -> dict[str, str]:
    return {"name": name, "status": "ok" if ok else "warn", "detail": detail}


def _line_filter(selector: str, needle: str) -> str:
    escaped = str(needle).replace("\\", "\\\\").replace('"', '\\"')
    return f'{selector} |= "{escaped}"'


def _parse_kv_segment(segment: str) -> dict[str, str]:
    text = str(segment or "").strip()
    if not text:
        return {}
    if " " not in text and "=" in text:
        key, value = text.split("=", 1)
        return {key: value}
    fields: dict[str, str] = {}
    try:
        tokens = shlex.split(text)
    except ValueError:
        tokens = text.split()
    for token in tokens:
        match = _KV_TOKEN.match(token)
        if match:
            fields[match.group("key")] = match.group("value")
    return fields


def _looks_like_event(message: str) -> bool:
    return bool(re.match(r"^[A-Za-z][A-Za-z0-9_:-]*$", str(message or "")))


def _normalize_time(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("time value is required")
    if text.isdigit():
        return text
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return _to_loki_time(parsed.astimezone(UTC))


def _to_loki_time(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _ns_to_iso(value: str) -> str:
    try:
        seconds = int(value) / 1_000_000_000
    except ValueError:
        return value
    return datetime.fromtimestamp(seconds, UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "DEFAULT_LOKI_URL",
    "DEFAULT_RUN_SELECTORS",
    "LokiClient",
    "doctor_log_payload",
    "parse_log_line",
    "query_log_payload",
    "run_log_payload",
]
