"""Regime overlay builder for BotLens playback."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from engines.bot_runtime.core.domain import Candle, normalize_epoch
from portal.backend.service.market.regime_blocks import build_regime_blocks
from portal.backend.service.market.regime_config import default_regime_runtime_config
logger = logging.getLogger(__name__)

STATE_COLORS = {
    "structure": {
        "trend": "#16a34a",
        "range": "#64748b",
        "transition": "#f59e0b",
        "chop": "#ef4444",
    },
    "expansion": {
        "expanding": "#22c55e",
        "compressing": "#a855f7",
        "stable": "#38bdf8",
    },
    "liquidity": {
        "heavy": "#22d3ee",
        "normal": "#64748b",
        "light": "#c084fc",
    },
    "volatility": {
        "high": "#f97316",
        "normal": "#0ea5e9",
        "quiet": "#38bdf8",
        "low": "#38bdf8",
    },
}


def _palette_for_lens(lens: str) -> Mapping[str, str]:
    key = (lens or "").strip().lower()
    return STATE_COLORS.get(key, {})


def state_color(state: Optional[str], *, lens: str = "structure") -> str:
    palette = _palette_for_lens(lens)
    key = (state or "").strip().lower()
    if not key or key in {"unknown", "none"}:
        return "#94a3b8"
    if key in palette:
        return palette[key]
    # stable deterministic fallback per lens to keep UX predictable when new states appear
    base_palette = [
        "#38bdf8",
        "#f59e0b",
        "#22c55e",
        "#c084fc",
        "#94a3b8",
    ]
    return base_palette[hash(key + lens) % len(base_palette)]


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


def _lens_band_bounds(lens: str, *, min_low: float, max_high: float) -> Tuple[float, float]:
    span = max(max_high - min_low, 1e-6)
    band_height = span * 0.035
    lens_key = (lens or "").strip().lower()
    band_positions = {
        "volatility": ("top", 0),
        "liquidity": ("top", 1),
        "expansion": ("top", 2),
    }
    side, offset = band_positions.get(lens_key, ("bottom", 0))
    if side == "top":
        y2 = max_high - (offset * band_height)
        y1 = y2 - band_height
        return y1, y2
    y1 = min_low + (offset * band_height)
    y2 = y1 + band_height
    return y1, y2


def _format_regime_label(block: Mapping[str, Any]) -> str:
    structure = (block.get("structure_state") or "unknown").title()
    volatility = (block.get("volatility_state") or "unknown").title()
    liquidity = (block.get("liquidity_state") or "unknown").title()
    expansion = (block.get("expansion_state") or "unknown").title()
    confidence = block.get("avg_confidence")
    confidence_label = ""
    if isinstance(confidence, (int, float)):
        confidence_label = f" (conf {float(confidence):.2f})"
    return f"{structure} • {volatility} Vol • {liquidity} • {expansion}{confidence_label}"


def detect_regime_changes(points: Sequence[Mapping[str, Any]]) -> List[int]:
    changes: List[int] = []
    last_state: Optional[str] = None
    for entry in points:
        state = (entry.get("regime_block_id") or entry.get("structure_state") or entry.get("state") or "")
        state = str(state).strip().lower()
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
        volatility = last_regime.get("volatility") if isinstance(last_regime, Mapping) else {}
        liquidity = last_regime.get("liquidity") if isinstance(last_regime, Mapping) else {}
        expansion = last_regime.get("expansion") if isinstance(last_regime, Mapping) else {}
        points.append(
            {
                "time": int(epoch),
                "candle_time": candle_time,
                "structure": structure,
                "structure_state": (structure or {}).get("state"),
                "expansion": expansion,
                "expansion_state": (expansion or {}).get("state"),
                "liquidity": liquidity,
                "liquidity_state": (liquidity or {}).get("state"),
                "volatility": volatility,
                "volatility_state": (volatility or {}).get("state"),
                "confidence": last_regime.get("confidence") if isinstance(last_regime, Mapping) else None,
                "regime_block_id": last_regime.get("regime_block_id") if isinstance(last_regime, Mapping) else None,
                "regime_key": last_regime.get("regime_key") if isinstance(last_regime, Mapping) else None,
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
        block_id = entry.get("regime_block_id")
        epoch = entry.get("time")
        if not isinstance(epoch, int):
            continue
        if not state:
            continue
        if last_state is None:
            last_state = block_id or state
            continue
        if (block_id or state) == last_state:
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
        last_state = block_id or state
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
    regime_config = default_regime_runtime_config()
    block_points = [
        {
            "time": entry.get("candle_time"),
            "structure_state": entry.get("structure_state"),
            "volatility_state": entry.get("volatility_state"),
            "liquidity_state": entry.get("liquidity_state"),
            "expansion_state": entry.get("expansion_state"),
            "confidence": entry.get("confidence"),
        }
        for entry in points
        if isinstance(entry.get("candle_time"), datetime)
    ]
    blocks, _ = build_regime_blocks(
        block_points,
        timeframe_seconds=timeframe_seconds,
        config=regime_config.blocks,
    )

    for block in blocks:
        start_epoch = _to_epoch(block.get("start_ts"))
        end_epoch = _to_epoch(block.get("end_ts"))
        known_at_epoch = _to_epoch(block.get("known_at"))
        if start_epoch is None or end_epoch is None:
            continue
        base_color = state_color(block.get("structure_state"))
        opacity = confidence_to_opacity(block.get("avg_confidence"))
        boxes.append(
            {
                "x1": int(start_epoch),
                "x2": int(end_epoch) + timeframe_seconds,
                "y1": min_low,
                "y2": max_high,
                "color": _to_rgba(base_color, opacity),
                "border": {"color": _to_rgba(base_color, min(opacity + 0.12, 0.4)), "width": 1},
                "precision": 4,
                "known_at": int(known_at_epoch) if known_at_epoch is not None else int(start_epoch),
                "state": block.get("structure_state"),
                "confidence": block.get("avg_confidence"),
                "regime_key": block.get("regime_key"),
                "block_id": block.get("block_id"),
            }
        )

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

    current_block = blocks[-1] if blocks else None
    regime_label = None
    if current_block:
        regime_label = _format_regime_label(current_block)
    payload: Dict[str, Any] = {
        "boxes": boxes,
        "segments": segments,
        "summary": {
            "regime_version": regime_version,
            "points": len(points),
            "changes": len(segments),
            "blocks": len(boxes),
            "current_regime_label": regime_label,
        },
    }
    if include_regime_points:
        payload["regime_points"] = list(points)
    return payload


def _build_lens_boxes(
    *,
    points: Sequence[Mapping[str, Any]],
    lens: str,
    min_low: float,
    max_high: float,
    timeframe_seconds: int,
) -> List[Dict[str, Any]]:
    boxes: List[Dict[str, Any]] = []
    regime_config = default_regime_runtime_config()
    state_key = f"{lens}_state"
    block_points = []
    for entry in points:
        lens_state = (entry.get(lens) or {}).get("state") if isinstance(entry.get(lens), Mapping) else None
        state = (entry.get(state_key) or lens_state or "").strip().lower()
        candle_time = entry.get("candle_time")
        if not isinstance(candle_time, datetime):
            continue
        block_points.append(
            {
                "time": candle_time,
                "structure_state": state,
                "confidence": entry.get("confidence"),
            }
        )

    if not block_points:
        return boxes

    blocks, _ = build_regime_blocks(
        block_points,
        timeframe_seconds=timeframe_seconds,
        config=regime_config.blocks,
    )
    y1, y2 = _lens_band_bounds(lens, min_low=min_low, max_high=max_high)
    for block in blocks:
        start_epoch = _to_epoch(block.get("start_ts"))
        end_epoch = _to_epoch(block.get("end_ts"))
        known_at_epoch = _to_epoch(block.get("known_at"))
        if start_epoch is None or end_epoch is None:
            continue
        state = block.get("structure_state")
        base_color = state_color(state, lens=lens)
        opacity = confidence_to_opacity(block.get("avg_confidence"))
        boxes.append(
            {
                "x1": int(start_epoch),
                "x2": int(end_epoch) + timeframe_seconds,
                "y1": y1,
                "y2": y2,
                "color": _to_rgba(base_color, opacity),
                "border": {"color": _to_rgba(base_color, min(opacity + 0.1, 0.32)), "width": 1},
                "precision": 4,
                "known_at": int(known_at_epoch) if known_at_epoch is not None else int(start_epoch),
                "state": state,
                "lens": lens,
                "confidence": block.get("avg_confidence"),
            }
        )

    return boxes


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

    lenses = [
        ("structure", True),
        ("expansion", True),
        ("liquidity", True),
        ("volatility", True),
    ]

    for lens, include in lenses:
        if not include:
            continue
        boxes = _build_lens_boxes(
            points=points,
            lens=lens,
            min_low=min_low,
            max_high=max_high,
            timeframe_seconds=timeframe_seconds,
        )
        if not boxes:
            continue
        overlay_payload = {
            "boxes": boxes,
            "summary": {
                "regime_version": regime_version,
                "lens": lens,
                "segments": 0,
                "boxes": len(boxes),
            },
        }
        overlays.append(build_overlay(f"regime_overlay_{lens}", overlay_payload))

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
