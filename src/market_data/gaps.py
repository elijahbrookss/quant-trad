"""Shared interval coverage rules for recorded market-data gaps."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def matching_gap_evidence(
    *,
    start: datetime,
    end: datetime,
    evidence: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in evidence
        if row.get("start") is not None
        and row.get("end") is not None
        and _utc(row["start"]) < end
        and _utc(row["end"]) > start
    ]


def recorded_gaps_cover_interval(
    *,
    start: datetime,
    end: datetime,
    evidence: Sequence[Mapping[str, Any]],
) -> bool:
    """Return true only when the union of recorded ranges covers all of [start, end)."""

    cursor = start
    ranges = sorted(
        (
            max(start, _utc(row["start"])),
            min(end, _utc(row["end"])),
        )
        for row in evidence
        if row.get("start") is not None and row.get("end") is not None
    )
    for range_start, range_end in ranges:
        if range_end <= range_start or range_end <= cursor:
            continue
        if range_start > cursor:
            return False
        cursor = max(cursor, range_end)
        if cursor >= end:
            return True
    return cursor >= end


__all__ = ["matching_gap_evidence", "recorded_gaps_cover_interval"]
