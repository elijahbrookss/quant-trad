"""Regime overlay builder for BotLens playback."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from engines.bot_runtime.core.domain import Candle, normalize_epoch
logger = logging.getLogger(__name__)

STATE_COLORS = {
    "trend": "#16a34a",
    "range": "#64748b",
    "transition": "#f59e0b",
    "chop": "#ef4444",
}


def state_color(state: Optional[str]) -> str:
    key = (state or "").strip().lower()
    return STATE_COLORS.get(key, "#94a3b8")


def confidence_to_opacity(confidence: Optional[float], *, min_alpha: float = 0.06, max_alpha: float = 0.22) -> float:
    if confidence is None:
        return min_alpha
    try:
        value = float(confidence)
    except (TypeError, ValueError):
        return min_alpha
    clamped = min(max(value, 0.0), 1.0)
    return min_alpha + (max_alpha - min_alpha) * clamped


def _hex_to_rgb(color: str) -> Optional[Tuple[int, int, int]]:
    if not color:
        return None
    text = color.strip().lstrip("#")
    if len(text) == 3:
        text = "".join([c * 2 for c in text])
    if len(text) != 6:
        return None
    try:
        return (int(text[0:2], 16), int(text[2:4], 16), int(text[4:6], 16))
    except ValueError:
        return None


def _to_rgba(color: str, alpha: float) -> str:
    rgb = _hex_to_rgb(color)
    if rgb is None:
        return color
    a = min(max(alpha, 0.0), 1.0)
    return f"rgba({rgb[0]},{rgb[1]},{rgb[2]},{a:.3f})"


def detect_regime_changes(points: Sequence[Mapping[str, Any]]) -> List[int]:
    changes: List[int] = []
    last_state: Optional[str] = None
    for entry in points:
        state = (entry.get("structure_state") or entry.get("state") or "").strip().lower()
        epoch = entry.get("time")
        if not isinstance(epoch, (int, float)):
            continue
        if last_state is None:
            last_state = state or None
            continue
        if state and state != last_state:
            changes.append(int(epoch))
        if state:
            last_state = state
    return changes


def _to_epoch(value: Any) -> Optional[int]:
    return normalize_epoch(value)


def _to_naive(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.replace(tzinfo=None)


def _build_regime_points(
    candles: Sequence[Candle],
    regime_rows: Mapping[datetime, Mapping[str, Any]],
) -> Tuple[List[Dict[str, Any]], float, float]:
    ordered = list(candles)
    min_low = min(candle.low for candle in ordered)
    max_high = max(candle.high for candle in ordered)

    points: List[Dict[str, Any]] = []
    last_regime: Optional[Mapping[str, Any]] = None
    for candle in ordered:
        candle_time = _to_naive(candle.time)
        regime = regime_rows.get(candle_time) if candle_time else None
        if regime is not None:
            last_regime = regime
        if last_regime is None:
            continue
        epoch = _to_epoch(candle.time)
        if epoch is None:
            continue
        structure = last_regime.get("structure") if isinstance(last_regime, Mapping) else {}
        points.append(
            {
                "time": int(epoch),
                "structure": structure,
                "structure_state": (structure or {}).get("state"),
                "expansion": last_regime.get("expansion") if isinstance(last_regime, Mapping) else {},
                "liquidity": last_regime.get("liquidity") if isinstance(last_regime, Mapping) else {},
                "volatility": last_regime.get("volatility") if isinstance(last_regime, Mapping) else {},
                "confidence": last_regime.get("confidence") if isinstance(last_regime, Mapping) else None,
            }
        )

    return points, min_low, max_high


def build_regime_markers(
    points: Sequence[Mapping[str, Any]],
    candles: Sequence[Candle],
) -> List[Dict[str, Any]]:
    price_by_time = {
        normalize_epoch(candle.time): candle.close
        for candle in candles
        if normalize_epoch(candle.time) is not None
    }
    markers: List[Dict[str, Any]] = []
    last_state: Optional[str] = None

    for entry in points:
        state = (entry.get("structure_state") or entry.get("state") or "").strip().lower()
        epoch = entry.get("time")
        if not isinstance(epoch, int):
            continue
        if not state:
            continue
        if last_state is None:
            last_state = state
            continue
        if state == last_state:
            continue
        price = price_by_time.get(epoch)
        if price is None:
            continue
        confidence = entry.get("confidence")
        confidence_label = ""
        if isinstance(confidence, (int, float)):
            confidence_label = f" ({round(float(confidence) * 100)}%)"
        markers.append(
            {
                "time": epoch,
                "price": price,
                "color": state_color(state),
                "shape": "circle",
                "size": 6,
                "text": f"{state.title()}{confidence_label}",
                "position": "aboveBar",
                "subtype": "regime_research",
            }
        )
        last_state = state
    logger.debug(
        "regime_markers_built | points=%s | markers=%s",
        len(points),
        len(markers),
    )
    return markers


def build_regime_marker_overlay(
    points: Sequence[Mapping[str, Any]],
    candles: Sequence[Candle],
) -> Optional[Dict[str, Any]]:
    markers = build_regime_markers(points, candles)
    if not markers:
        return None
    from signals.overlays.schema import build_overlay

    return build_overlay("regime_markers", {"markers": markers})


def _build_regime_payload(
    points: Sequence[Mapping[str, Any]],
    *,
    min_low: float,
    max_high: float,
    timeframe_seconds: int,
    regime_version: Optional[str],
    include_change_markers: bool,
    include_regime_points: bool,
) -> Dict[str, Any]:
    boxes: List[Dict[str, Any]] = []
    current_state = None
    current_start = None
    confidence_sum = 0.0
    confidence_count = 0

    def flush_box(end_epoch: int) -> None:
        nonlocal current_state, current_start, confidence_sum, confidence_count
        if current_state is None or current_start is None:
            return
        avg_conf = confidence_sum / confidence_count if confidence_count else None
        base_color = state_color(current_state)
        opacity = confidence_to_opacity(avg_conf)
        boxes.append(
            {
                "x1": current_start,
                "x2": end_epoch + timeframe_seconds,
                "y1": min_low,
                "y2": max_high,
                "color": _to_rgba(base_color, opacity),
                "border": {"color": _to_rgba(base_color, min(opacity + 0.12, 0.4)), "width": 1},
                "precision": 4,
                "known_at": current_start,
                "state": current_state,
                "confidence": avg_conf,
            }
        )
        current_state = None
        current_start = None
        confidence_sum = 0.0
        confidence_count = 0

    for idx, entry in enumerate(points):
        state = (entry.get("structure_state") or "").strip().lower()
        epoch = entry.get("time")
        if not isinstance(epoch, int):
            continue
        confidence = entry.get("confidence")
        if current_state is None:
            current_state = state or None
            current_start = epoch
        if state and state != current_state:
            flush_box(epoch)
            current_state = state
            current_start = epoch
        if isinstance(confidence, (int, float)):
            confidence_sum += float(confidence)
            confidence_count += 1
        if idx == len(points) - 1:
            flush_box(epoch)

    segments: List[Dict[str, Any]] = []
    if include_change_markers:
        for epoch in detect_regime_changes(points):
            segments.append(
                {
                    "x1": epoch,
                    "x2": epoch,
                    "y1": min_low,
                    "y2": max_high,
                    "color": "rgba(148,163,184,0.45)",
                    "lineWidth": 1,
                    "lineStyle": 2,
                }
            )

    payload: Dict[str, Any] = {
        "boxes": boxes,
        "segments": segments,
        "summary": {
            "regime_version": regime_version,
            "points": len(points),
            "changes": len(segments),
        },
    }
    if include_regime_points:
        payload["regime_points"] = list(points)
    return payload


def build_regime_overlay(
    *,
    candles: Sequence[Candle],
    regime_rows: Mapping[datetime, Mapping[str, Any]],
    timeframe_seconds: int,
    regime_version: Optional[str] = None,
    include_change_markers: bool = True,
    include_regime_points: bool = False,
) -> Optional[Dict[str, Any]]:
    if not candles or not regime_rows:
        return None

    points, min_low, max_high = _build_regime_points(candles, regime_rows)

    if not points:
        return None

    payload = _build_regime_payload(
        points,
        min_low=min_low,
        max_high=max_high,
        timeframe_seconds=timeframe_seconds,
        regime_version=regime_version,
        include_change_markers=include_change_markers,
        include_regime_points=include_regime_points,
    )

    from signals.overlays.schema import build_overlay

    return build_overlay("regime_overlay", payload)


def build_regime_overlays(
    *,
    candles: Sequence[Candle],
    regime_rows: Mapping[datetime, Mapping[str, Any]],
    timeframe_seconds: int,
    regime_version: Optional[str] = None,
    include_change_markers: bool = True,
    include_marker_overlay: bool = True,
) -> List[Dict[str, Any]]:
    if not candles or not regime_rows:
        return []

    points, min_low, max_high = _build_regime_points(candles, regime_rows)
    if not points:
        return []

    payload = _build_regime_payload(
        points,
        min_low=min_low,
        max_high=max_high,
        timeframe_seconds=timeframe_seconds,
        regime_version=regime_version,
        include_change_markers=include_change_markers,
        include_regime_points=False,
    )
    if include_change_markers:
        change_epochs = detect_regime_changes(points)
        logger.debug(
            "regime_overlay_change_markers | points=%s | changes=%s | epochs=%s",
            len(points),
            len(change_epochs),
            change_epochs[:12],
        )
    from signals.overlays.schema import build_overlay

    overlays = [build_overlay("regime_overlay", payload)]
    if include_marker_overlay:
        marker_overlay = build_regime_marker_overlay(points, candles)
        if marker_overlay:
            overlays.append(marker_overlay)
    return overlays


__all__ = [
    "STATE_COLORS",
    "state_color",
    "confidence_to_opacity",
    "detect_regime_changes",
    "build_regime_markers",
    "build_regime_marker_overlay",
    "build_regime_overlays",
    "build_regime_overlay",
]
