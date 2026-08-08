"""Immutable provider-range coverage semantics for sparse market facts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from .frozen import semantic_hash


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


def _iso(value: Any) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def normalize_acquisition_coverage(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Validate and detach exact immutable coverage records."""

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in rows:
        row = dict(raw)
        identity_key = str(row.get("identity_key") or "").strip()
        source_identity_key = str(row.get("source_identity_key") or "").strip()
        if not identity_key or identity_key in seen or not source_identity_key:
            raise ValueError(
                "market_acquisition_coverage_invalid: coverage identity is missing or duplicated"
            )
        seen.add(identity_key)
        material = {
            "schema_version": "market.fact_acquisition_coverage.v1",
            "series_id": int(row["series_id"]),
            "source_id": int(row["source_id"]),
            "binding_id": str(row["binding_id"]),
            "manifest_hash": str(row["manifest_hash"]).lower(),
            "interface_version": str(row["interface_version"]),
            "confirmation_depth": int(row["confirmation_depth"]),
            "range_start": _iso(row["range_start"]),
            "range_end": _iso(row["range_end"]),
            "source_positions": {
                "start": str(row["source_position_start"]),
                "end": str(row["source_position_end"]),
                "head": str(row.get("source_position_head") or "").strip() or None,
            },
            "status": str(row["status"]).strip().lower(),
            "evidence": dict(row.get("evidence") or {}),
        }
        if semantic_hash(material) != identity_key:
            raise ValueError(
                "market_acquisition_coverage_invalid: coverage hash disagreement"
            )
        normalized.append(
            {
                **row,
                **material,
                "identity_key": identity_key,
                "source_identity_key": source_identity_key,
                "created_at": _iso(row["created_at"]),
            }
        )
    return sorted(
        normalized,
        key=lambda row: (
            row["source_identity_key"],
            row["range_start"],
            row["range_end"],
            row["identity_key"],
        ),
    )


def missing_complete_coverage(
    *,
    start: Any,
    end: Any,
    source_identity_keys: Sequence[str],
    coverage: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    """Return every unproven provider subrange, independently per source."""

    lower = _utc(start)
    upper = _utc(end)
    if upper <= lower:
        raise ValueError("market_acquisition_coverage_invalid: range is empty")
    normalized = normalize_acquisition_coverage(coverage)
    missing: list[dict[str, str]] = []
    for source_key in sorted(
        {str(value).strip() for value in source_identity_keys if str(value).strip()}
    ):
        ranges = sorted(
            (
                max(lower, _utc(row["range_start"])),
                min(upper, _utc(row["range_end"])),
            )
            for row in normalized
            if row["source_identity_key"] == source_key
            and row["status"] == "complete"
            and _utc(row["range_end"]) > lower
            and _utc(row["range_start"]) < upper
        )
        merged: list[tuple[datetime, datetime]] = []
        for range_start, range_end in ranges:
            if range_end <= range_start:
                continue
            if not merged or range_start > merged[-1][1]:
                merged.append((range_start, range_end))
            else:
                merged[-1] = (merged[-1][0], max(merged[-1][1], range_end))
        cursor = lower
        for range_start, range_end in merged:
            if range_start > cursor:
                missing.append(
                    {
                        "source_identity_key": source_key,
                        "start": _iso(cursor),
                        "end": _iso(range_start),
                    }
                )
            cursor = max(cursor, range_end)
        if cursor < upper:
            missing.append(
                {
                    "source_identity_key": source_key,
                    "start": _iso(cursor),
                    "end": _iso(upper),
                }
            )
    return missing


__all__ = ["missing_complete_coverage", "normalize_acquisition_coverage"]
