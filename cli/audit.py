from __future__ import annotations

from datetime import UTC, datetime
import json
import re
import time
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence


_SAFE_PATH_PART = re.compile(r"[^A-Za-z0-9_.=-]+")
_SENSITIVE_FLAGS = {
    "--secret",
    "--secrets-json",
}
_SENSITIVE_ARG_NAMES = {
    "credentials",
    "secret",
    "secrets",
    "secrets_json",
}


def utc_now() -> datetime:
    return datetime.now(UTC)


def timestamp_slug(value: datetime | None = None) -> str:
    current = value or utc_now()
    return current.strftime("%Y%m%dT%H%M%S.%fZ")


def date_partition(value: datetime | None = None) -> Path:
    current = value or utc_now()
    return Path(f"{current:%Y}") / f"{current:%m}" / f"{current:%d}"


def safe_path_part(value: Any, *, fallback: str = "unknown") -> str:
    text = str(value or "").strip()
    if not text:
        text = fallback
    text = _SAFE_PATH_PART.sub("-", text).strip(".-")
    return text or fallback


def command_path(args: Any) -> str:
    parts = [
        getattr(args, "command", None),
        getattr(args, "bots_command", None),
        getattr(args, "runs_command", None),
        getattr(args, "strategies_command", None),
        getattr(args, "variants_command", None),
        getattr(args, "reports_command", None),
        getattr(args, "providers_command", None),
        getattr(args, "credentials_command", None),
        getattr(args, "experiments_command", None),
        getattr(args, "mcp_command", None),
    ]
    return ".".join(safe_path_part(part) for part in parts if part)


def report_export_dir(root: str | Path, *, run_id: str, now: datetime | None = None) -> Path:
    return Path(root).expanduser() / date_partition(now) / f"run_{safe_path_part(run_id)}"


def experiment_dir(root: str | Path, *, experiment_id: str, now: datetime | None = None) -> Path:
    return Path(root).expanduser() / "experiments" / date_partition(now) / safe_path_part(experiment_id)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _redact_sensitive(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): "***REDACTED***" if str(key) in _SENSITIVE_ARG_NAMES else _redact_sensitive(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_redact_sensitive(item) for item in value]
    return _json_safe(value)


def _namespace_payload(args: Any) -> dict[str, Any]:
    payload = {
        key: value
        for key, value in vars(args).items()
        if key not in {"func"} and not callable(value)
    }
    return _redact_sensitive(payload)


def _redact_argv(argv: Sequence[str]) -> list[str]:
    redacted: list[str] = []
    redact_next = False
    for item in argv:
        if redact_next:
            redacted.append("***REDACTED***")
            redact_next = False
            continue
        if any(item == flag for flag in _SENSITIVE_FLAGS):
            redacted.append(item)
            redact_next = True
            continue
        if any(item.startswith(f"{flag}=") for flag in _SENSITIVE_FLAGS):
            redacted.append(item.split("=", 1)[0] + "=***REDACTED***")
            continue
        redacted.append(item)
    return redacted


class CliAuditLog:
    """One structured audit file for one CLI invocation."""

    def __init__(
        self,
        *,
        root: str | Path,
        args: Any,
        argv: Sequence[str],
        enabled: bool = True,
    ) -> None:
        self.enabled = bool(enabled)
        self.started_at = utc_now()
        self.started_monotonic = time.monotonic()
        self.operation_id = f"{timestamp_slug(self.started_at)}-{uuid.uuid4().hex[:8]}"
        self.command = command_path(args) or "unknown"
        self.root = Path(root).expanduser()
        self.argv = _redact_argv(argv)
        self.args = _namespace_payload(args)
        self.events: list[dict[str, Any]] = []
        self.artifacts: list[dict[str, Any]] = []

    @property
    def path(self) -> Path:
        return (
            self.root
            / "cli"
            / date_partition(self.started_at)
            / Path(*self.command.split("."))
            / f"{self.operation_id}.json"
        )

    def record_event(self, event: str, **fields: Any) -> None:
        if not self.enabled:
            return
        self.events.append(
            {
                "event": event,
                "at": utc_now().isoformat(),
                **_json_safe(fields),
            }
        )

    def record_artifact(self, kind: str, path: str | Path, **fields: Any) -> None:
        if not self.enabled:
            return
        payload = {
            "kind": kind,
            "path": str(path),
            **_json_safe(fields),
        }
        self.artifacts.append(payload)
        self.record_event("artifact_written", **payload)

    def finish(self, *, exit_code: int, error: Mapping[str, Any] | None = None) -> Path | None:
        if not self.enabled:
            return None
        finished_at = utc_now()
        payload = {
            "operation_id": self.operation_id,
            "command": self.command,
            "argv": self.argv,
            "args": self.args,
            "started_at": self.started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_ms": round((time.monotonic() - self.started_monotonic) * 1000, 3),
            "exit_code": int(exit_code),
            "events": self.events,
            "artifacts": self.artifacts,
        }
        if error:
            payload["error"] = _json_safe(dict(error))
        path = self.path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
        return path
