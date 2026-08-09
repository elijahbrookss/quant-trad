from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Dict, Iterable

from .botlens_retrieval_queries import DomainTruthEvent
from .botlens_state import apply_overlay_delta, project_overlay_state


_TERMINAL_RUN_STATUSES = {"completed", "cancelled", "canceled", "failed", "stopped"}
_POINT_GEOMETRY_KEYS = ("markers", "bubbles", "touch_points", "touchPoints")
_INTERVAL_GEOMETRY_KEYS = ("boxes", "segments", "regime_blocks")
_MAX_OVERLAY_EVENTS = 5_000


def _mapping(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _epoch(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 1_000_000_000_000:
            number /= 1000.0
        return int(number)
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.astimezone(timezone.utc).timestamp())


def _iso(epoch: int | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _stable_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _point_time(value: Mapping[str, Any]) -> int | None:
    for field in ("time", "timestamp", "bar_time", "known_at"):
        resolved = _epoch(value.get(field))
        if resolved is not None:
            return resolved
    return None


def _interval(value: Mapping[str, Any], *, default_end: int) -> tuple[int | None, int | None]:
    start = None
    end = None
    for field in ("x1", "start", "start_time", "start_ts", "originTime"):
        start = _epoch(value.get(field))
        if start is not None:
            break
    for field in ("x2", "end", "end_time", "end_ts", "endTime"):
        end = _epoch(value.get(field))
        if end is not None:
            break
    if start is not None and end is None:
        end = int(default_end)
    return start, end


def _clip_interval_entry(
    value: Any,
    *,
    start_epoch: int,
    end_epoch: int,
) -> Dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    interval_start, interval_end = _interval(value, default_end=end_epoch)
    if interval_start is None or interval_end is None:
        return None
    low = min(interval_start, interval_end)
    high = max(interval_start, interval_end)
    if high < start_epoch or low >= end_epoch:
        return None
    clipped = dict(value)
    clipped["x1"] = max(low, start_epoch)
    clipped["x2"] = min(high, end_epoch)
    return clipped


def _clip_price_lines(
    values: Any,
    *,
    start_epoch: int,
    end_epoch: int,
) -> list[Dict[str, Any]]:
    segments: list[Dict[str, Any]] = []
    for value in values if isinstance(values, list) else []:
        if not isinstance(value, Mapping):
            continue
        price = value.get("price")
        try:
            resolved_price = float(price)
        except (TypeError, ValueError):
            continue
        origin = _epoch(value.get("originTime"))
        line_end = _epoch(value.get("endTime"))
        interval_start = origin if origin is not None else start_epoch
        interval_end = line_end if line_end is not None else end_epoch
        if interval_end < start_epoch or interval_start >= end_epoch:
            continue
        segments.append(
            {
                "x1": max(interval_start, start_epoch),
                "x2": min(interval_end, end_epoch),
                "y1": resolved_price,
                "y2": resolved_price,
                "color": value.get("color"),
                "lineWidth": value.get("lineWidth"),
                "lineStyle": value.get("lineStyle"),
                "role": "historical_price_line",
                "title": value.get("title"),
            }
        )
    return [
        {key: entry for key, entry in segment.items() if entry not in (None, "")}
        for segment in segments
    ]


def _clip_overlay_payload(
    payload: Mapping[str, Any],
    *,
    start_epoch: int,
    end_epoch: int,
) -> Dict[str, Any]:
    clipped = {
        str(key): value
        for key, value in payload.items()
        if key
        not in {
            *_POINT_GEOMETRY_KEYS,
            *_INTERVAL_GEOMETRY_KEYS,
            "polylines",
            "price_lines",
        }
    }
    for key in _POINT_GEOMETRY_KEYS:
        values = payload.get(key)
        if not isinstance(values, list):
            continue
        retained = []
        for value in values:
            if not isinstance(value, Mapping):
                continue
            point_epoch = _point_time(value)
            if point_epoch is not None and start_epoch <= point_epoch < end_epoch:
                retained.append(dict(value))
        clipped[key] = retained

    interval_segments: list[Dict[str, Any]] = []
    for key in _INTERVAL_GEOMETRY_KEYS:
        values = payload.get(key)
        if not isinstance(values, list):
            continue
        retained = [
            result
            for result in (
                _clip_interval_entry(
                    value,
                    start_epoch=start_epoch,
                    end_epoch=end_epoch,
                )
                for value in values
            )
            if result is not None
        ]
        if key == "segments":
            interval_segments.extend(retained)
        else:
            clipped[key] = retained

    interval_segments.extend(
        _clip_price_lines(
            payload.get("price_lines"),
            start_epoch=start_epoch,
            end_epoch=end_epoch,
        )
    )
    clipped["segments"] = interval_segments

    polylines = []
    for value in payload.get("polylines") if isinstance(payload.get("polylines"), list) else []:
        if not isinstance(value, Mapping):
            continue
        raw_points = value.get("points") if isinstance(value.get("points"), list) else []
        points = [
            dict(point)
            for point in raw_points
            if isinstance(point, Mapping)
            and (point_epoch := _point_time(point)) is not None
            and start_epoch <= point_epoch < end_epoch
        ]
        if points:
            polylines.append({**dict(value), "points": points})
    clipped["polylines"] = polylines
    return clipped


def _payload_has_geometry(payload: Mapping[str, Any]) -> bool:
    return any(
        isinstance(payload.get(key), list) and bool(payload.get(key))
        for key in (
            *_POINT_GEOMETRY_KEYS,
            *_INTERVAL_GEOMETRY_KEYS,
            "polylines",
            "price_lines",
        )
    )


def _clip_overlays(
    overlays: Sequence[Mapping[str, Any]],
    *,
    start_epoch: int,
    end_epoch: int,
) -> list[Dict[str, Any]]:
    clipped: list[Dict[str, Any]] = []
    page_key = f"{start_epoch}:{end_epoch}"
    for index, overlay in enumerate(overlays):
        payload = _mapping(overlay.get("payload"))
        clipped_payload = _clip_overlay_payload(
            payload,
            start_epoch=start_epoch,
            end_epoch=end_epoch,
        )
        if not _payload_has_geometry(clipped_payload):
            continue
        source_overlay_id = str(
            overlay.get("source_overlay_id")
            or overlay.get("overlay_id")
            or overlay.get("id")
            or f"index:{index}"
        )
        clipped.append(
            {
                **dict(overlay),
                "source_overlay_id": source_overlay_id,
                "overlay_id": f"history:{page_key}:{source_overlay_id}",
                "detail_level": "bounded_historical_render",
                "payload": clipped_payload,
            }
        )
    return clipped


def build_chart_overlay_history(
    *,
    events: Iterable[DomainTruthEvent],
    symbol_key: str,
    run_status: str,
    range_start_epoch: int,
    range_end_epoch: int,
    timeframe_seconds: int,
    has_more_after: bool,
) -> tuple[list[Dict[str, Any]], Dict[str, Any]]:
    rows = sorted(
        list(events),
        key=lambda event: (
            _int(_mapping(_mapping(event.context).get("overlay_delta")).get("overlay_commit_seq"), 0),
            str(getattr(event, "event_id", "") or ""),
        ),
    )
    if len(rows) > _MAX_OVERLAY_EVENTS:
        raise RuntimeError(
            "botlens_chart_overlay_history_limit_exceeded: "
            f"symbol_key={symbol_key} event_count={len(rows)} limit={_MAX_OVERLAY_EVENTS}"
        )
    if not rows:
        return [], {
            "schema_version": "botlens_chart_overlay_evidence.v2",
            "source": "domain_event_ledger",
            "coverage": "unavailable",
            "complete_for_returned_candles": False,
            "ordering_assured": False,
            "reason_codes": ["overlay_timeline_not_retained"],
            "event_count": 0,
            "overlay_count": 0,
            "fingerprint": None,
        }

    overlays: tuple[Dict[str, Any], ...] = ()
    expected_overlay_seq = 0
    previous_bar_epoch: int | None = None
    ordering_assured = True
    reason_codes: list[str] = []
    applied_events = 0
    first_overlay_seq: int | None = None
    latest_projection: Dict[str, Any] = {}
    projected_through: int | None = None
    # Replay owns this state exclusively. Each cached value is the computed and
    # validated result of the preceding tail patch; non-tail mutations evict it.
    verified_polyline_fingerprints: dict[str, str] = {}

    for event in rows:
        context = _mapping(event.context)
        delta = _mapping(context.get("overlay_delta"))
        overlay_seq = _int(delta.get("overlay_commit_seq"), 0)
        base_overlay_seq = _int(delta.get("base_overlay_commit_seq"), -1)
        status = str(delta.get("overlay_commit_seq_status") or "").strip()
        bar_epoch = _epoch(context.get("bar_time") or event.event_ts)
        run_ordered = (
            str(context.get("run_seq_status") or "") == "runtime_assigned"
            and _int(context.get("run_seq"), 0) > 0
        )
        clock_ordered = (
            status == "overlay_scoped"
            and overlay_seq == expected_overlay_seq + 1
            and base_overlay_seq == expected_overlay_seq
        )
        time_ordered = (
            bar_epoch is not None
            and (previous_bar_epoch is None or bar_epoch >= previous_bar_epoch)
        )
        if not (run_ordered and clock_ordered and time_ordered):
            ordering_assured = False
            reason_codes.append("overlay_timeline_gap_or_order_violation")
            break
        try:
            overlays = apply_overlay_delta(
                overlays,
                delta,
                defer_revisions=True,
                verified_polyline_fingerprints=verified_polyline_fingerprints,
            )
        except ValueError as exc:
            ordering_assured = False
            reason_codes.append(f"overlay_delta_invalid:{exc}")
            break
        expected_overlay_seq = overlay_seq
        first_overlay_seq = overlay_seq if first_overlay_seq is None else first_overlay_seq
        previous_bar_epoch = bar_epoch
        projected_through = bar_epoch
        latest_projection = _mapping(delta.get("projection"))
        applied_events += 1

    window_bars = _int(latest_projection.get("window_bars"), 0)
    emit_every_bars = _int(latest_projection.get("emit_every_bars"), 0)
    window_start = (
        projected_through - max(window_bars - 1, 0) * int(timeframe_seconds)
        if projected_through is not None and window_bars > 0
        else None
    )
    returned_last_epoch = int(range_end_epoch) - int(timeframe_seconds)
    cadence_lag_bars = (
        max(0, (returned_last_epoch - projected_through) // max(int(timeframe_seconds), 1))
        if projected_through is not None
        else None
    )
    window_covers_range = (
        window_start is not None and int(range_start_epoch) >= int(window_start)
    )
    cadence_covers_range = (
        cadence_lag_bars is not None
        and emit_every_bars > 0
        and cadence_lag_bars < emit_every_bars
    )
    terminal_run = str(run_status or "").strip().lower() in _TERMINAL_RUN_STATUSES
    terminal_checkpoint_required = terminal_run and not bool(has_more_after)
    terminal_checkpoint_present = bool(latest_projection.get("terminal"))
    if not window_covers_range:
        reason_codes.append("returned_range_exceeds_overlay_window")
    if not cadence_covers_range:
        reason_codes.append("overlay_projection_cadence_gap")
    if terminal_checkpoint_required and not terminal_checkpoint_present:
        reason_codes.append("terminal_overlay_checkpoint_missing")

    complete = bool(
        ordering_assured
        and applied_events == len(rows)
        and window_covers_range
        and cadence_covers_range
        and (not terminal_checkpoint_required or terminal_checkpoint_present)
    )
    projected_overlays = project_overlay_state(overlays)
    clipped = _clip_overlays(
        projected_overlays,
        start_epoch=int(range_start_epoch),
        end_epoch=int(range_end_epoch),
    )
    payload_truncated = any(
        bool(_mapping(overlay.get("payload_summary")).get("truncated"))
        for overlay in projected_overlays
    )
    if payload_truncated:
        reason_codes.append("overlay_payload_truncated")
        complete = False
    fingerprint = _stable_hash(
        {
            "schema_version": "botlens_chart_overlay_evidence.v2",
            "symbol_key": symbol_key,
            "range_start_epoch": int(range_start_epoch),
            "range_end_epoch": int(range_end_epoch),
            "first_overlay_seq": first_overlay_seq,
            "last_overlay_seq": expected_overlay_seq,
            "projected_through": projected_through,
            "overlays": clipped,
        }
    )
    return clipped, {
        "schema_version": "botlens_chart_overlay_evidence.v2",
        "source": "domain_event_ledger",
        "coverage": "complete" if complete else "bounded",
        "complete_for_returned_candles": complete,
        "ordering_assured": ordering_assured,
        "reason_codes": sorted(set(reason_codes)),
        "event_count": len(rows),
        "applied_event_count": applied_events,
        "overlay_count": len(clipped),
        "payload_truncated": payload_truncated,
        "first_overlay_commit_seq": first_overlay_seq,
        "last_overlay_commit_seq": expected_overlay_seq or None,
        "projected_through": _iso(projected_through),
        "window_start": _iso(window_start),
        "window_bars": window_bars or None,
        "emit_every_bars": emit_every_bars or None,
        "cadence_lag_bars": cadence_lag_bars,
        "terminal_checkpoint_required": terminal_checkpoint_required,
        "terminal_checkpoint_present": terminal_checkpoint_present,
        "fingerprint": fingerprint,
    }


__all__ = ["build_chart_overlay_history"]
