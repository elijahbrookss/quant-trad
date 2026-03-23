"""Canonical signal-output item contract helpers."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Mapping, TypedDict

try:  # Python <3.11 compatibility
    from typing import NotRequired
except ImportError:  # pragma: no cover - executed on Python <3.11
    from typing_extensions import NotRequired


_EXECUTION_FIELDS = (
    "action_time",
    "entry_time",
    "fill_time",
    "planned_entry_time",
    "next_open_time",
    "next_bar_open_time",
)

_ALLOWED_EVENT_KEYS = {
    "key",
    "direction",
    "pattern_id",
    "known_at",
    "confidence",
    "metadata",
}


class SignalOutputEvent(TypedDict):
    key: str
    direction: NotRequired[str]
    pattern_id: NotRequired[str]
    known_at: NotRequired[int | float | str | datetime]
    confidence: NotRequired[float]
    metadata: NotRequired[Mapping[str, Any]]


def _to_epoch(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        numeric = float(value)
        return int(numeric) if math.isfinite(numeric) else None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    dt = parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _metadata(event: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = event.get("metadata")
    return metadata if isinstance(metadata, Mapping) else {}


def assert_signal_output_event(event: Mapping[str, Any]) -> None:
    """Raise if a signal-output item violates the canonical item shape."""

    if not isinstance(event, Mapping):
        raise RuntimeError("indicator_signal_output_invalid: event must be a mapping")

    unexpected_keys = sorted(str(key) for key in event.keys() if key not in _ALLOWED_EVENT_KEYS)
    if unexpected_keys:
        raise RuntimeError(
            "indicator_signal_output_invalid: unexpected keys "
            f"({','.join(unexpected_keys)})"
        )

    key = event.get("key")
    if not isinstance(key, str) or not key.strip():
        raise RuntimeError("indicator_signal_output_invalid: missing key")

    direction = event.get("direction")
    if direction is not None and (not isinstance(direction, str) or not direction.strip()):
        raise RuntimeError("indicator_signal_output_invalid: direction must be non-empty string")

    pattern_id = event.get("pattern_id")
    if pattern_id is not None and (not isinstance(pattern_id, str) or not pattern_id.strip()):
        raise RuntimeError("indicator_signal_output_invalid: pattern_id must be non-empty string")

    known_at = event.get("known_at")
    if known_at is not None and _to_epoch(known_at) is None:
        raise RuntimeError("indicator_signal_output_invalid: invalid known_at")

    confidence = event.get("confidence")
    if confidence is not None:
        try:
            numeric = float(confidence)
        except (TypeError, ValueError):
            raise RuntimeError("indicator_signal_output_invalid: confidence must be numeric") from None
        if not math.isfinite(numeric):
            raise RuntimeError("indicator_signal_output_invalid: confidence must be finite")

    metadata = event.get("metadata")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise RuntimeError("indicator_signal_output_invalid: metadata must be mapping")


def assert_signal_output_has_no_execution_fields(event: Mapping[str, Any], *, mode: str = "raise") -> list[str]:
    """Guard the signal-output boundary from execution-timing leakage."""

    leaks: list[str] = []
    metadata = _metadata(event)
    for field in _EXECUTION_FIELDS:
        if event.get(field) not in (None, "") or metadata.get(field) not in (None, ""):
            leaks.append(field)
    if leaks and mode == "raise":
        joined = ",".join(sorted(set(leaks)))
        raise RuntimeError(
            f"indicator_signal_output_invalid: execution fields not allowed ({joined})"
        )
    return leaks


def signal_output_known_at_epoch(event: Mapping[str, Any]) -> int | None:
    """Return the normalized known-at timestamp when present."""

    return _to_epoch(event.get("known_at"))


__all__ = [
    "SignalOutputEvent",
    "assert_signal_output_event",
    "assert_signal_output_has_no_execution_fields",
    "signal_output_known_at_epoch",
]
