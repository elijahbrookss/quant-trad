"""Existing analytical clocks; no alternative availability policy."""
from __future__ import annotations
from datetime import UTC, datetime
from typing import Any

def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"event_fact_check_invalid: {field} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                f"event_fact_check_invalid: {field} must be ISO-8601"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _known_at(value: Any, *, field: str) -> datetime:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(float(value), tz=UTC)
    raw = str(value or "").strip()
    if raw and raw.replace(".", "", 1).isdigit():
        return datetime.fromtimestamp(float(raw), tz=UTC)
    return _utc(value, field=field)


