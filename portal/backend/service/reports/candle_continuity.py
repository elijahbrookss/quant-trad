"""Pure reporting-time candle gap classification from closure evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, Optional


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if value else None


def _gap_missing_window(gap: Mapping[str, Any]) -> tuple[Optional[datetime], Optional[datetime]]:
    previous_ts = _parse_iso(gap.get("previous_ts") or gap.get("previous_time") or gap.get("previous"))
    current_ts = _parse_iso(gap.get("current_ts") or gap.get("current_time") or gap.get("current"))
    expected_seconds = _safe_int(gap.get("expected_interval_seconds"))
    if previous_ts is not None and current_ts is not None and expected_seconds and expected_seconds > 0:
        return previous_ts + timedelta(seconds=int(expected_seconds)), current_ts
    return (
        _parse_iso(gap.get("start") or gap.get("start_ts") or gap.get("missing_start")),
        _parse_iso(gap.get("end") or gap.get("end_ts") or gap.get("missing_end")),
    )


def _closure_covers_gap(closure: Mapping[str, Any], gap_start: datetime, gap_end: datetime) -> bool:
    closure_start = _parse_iso(closure.get("start") or closure.get("start_ts"))
    closure_end = _parse_iso(closure.get("end") or closure.get("end_ts"))
    if closure_start is None or closure_end is None:
        return False
    return closure_start <= gap_start and closure_end >= gap_end


def classify_unknown_gaps_from_closures(
    gaps: Sequence[Any],
    closures: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Reclassify fully closure-covered unknown gaps without mutating inputs.

    Input gap order and closure order are significant. The first closure that
    fully covers a gap supplies the provider evidence, matching the historical
    RunResearchDataset behavior.
    """

    normalized = [dict(gap) for gap in gaps if isinstance(gap, Mapping)]
    if not normalized or not closures:
        return normalized

    reclassified: list[dict[str, Any]] = []
    for gap in normalized:
        classification = str(gap.get("classification") or "unknown_gap")
        if classification != "unknown_gap":
            reclassified.append(gap)
            continue
        gap_start, gap_end = _gap_missing_window(gap)
        if gap_start is None or gap_end is None:
            reclassified.append(gap)
            continue
        closure = next((row for row in closures if _closure_covers_gap(row, gap_start, gap_end)), None)
        if closure is None:
            reclassified.append(gap)
            continue
        closure_metadata = _mapping(closure.get("metadata"))
        provider_evidence = _mapping(closure_metadata.get("provider_evidence"))
        evidence = {
            **gap,
            "classification": "provider_missing_data",
            "reason_code": str(closure_metadata.get("reason_code") or "source_sparse"),
            "evidence": str(closure_metadata.get("evidence") or "portal_candle_closure"),
            "closure_start": _iso(_parse_iso(closure.get("start"))),
            "closure_end": _iso(_parse_iso(closure.get("end"))),
        }
        if provider_evidence:
            evidence["provider_evidence"] = provider_evidence
        reclassified.append(evidence)
    return reclassified
