"""Regime overlay builders owned by the regime indicator package."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from engines.bot_runtime.core.domain import Candle, normalize_epoch
from overlays.schema import build_overlay

from .blocks import build_regime_blocks
from .config import RegimeBlockConfig, default_regime_runtime_config

logger = logging.getLogger(__name__)

STATE_COLORS = {
    "structure": {
        "trend_up": "#16a34a",
        "trend_down": "#dc2626",
        "range": "#64748b",
        "transition_up": "#f59e0b",
        "transition_down": "#f59e0b",
        "transition_neutral": "#f59e0b",
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
        "thin": "#c084fc",
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
    return STATE_COLORS.get((lens or "").strip().lower(), {})


def state_color(state: Optional[str], *, lens: str = "structure", trend_direction: Optional[str] = None) -> str:
    key = (state or "").strip().lower()
    if lens == "structure":
        if key == "trend":
            direction = (trend_direction or "").strip().lower()
            if direction == "down":
                return "#dc2626"
            if direction == "up":
                return "#16a34a"
            return "#22c55e"
        if key == "transition":
            return "#f59e0b"
    palette = _palette_for_lens(lens)
    if not key or key in {"unknown", "none"}:
        return "#94a3b8"
    if key in palette:
        return palette[key]
    base_palette = ["#38bdf8", "#f59e0b", "#22c55e", "#c084fc", "#94a3b8"]
    return base_palette[hash(key + lens) % len(base_palette)]


def confidence_to_opacity(
    confidence: Optional[float],
    *,
    min_alpha: float = 0.06,
    max_alpha: float = 0.22,
) -> float:
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
    return f"rgba({rgb[0]},{rgb[1]},{rgb[2]},{min(max(alpha, 0.0), 1.0):.3f})"


def _lens_band_bounds(lens: str, *, min_low: float, max_high: float) -> Tuple[float, float]:
    span = max(max_high - min_low, 1e-6)
    band_height = span * 0.055
    band_positions = {
        "volatility": ("top", 0),
        "liquidity": ("top", 1),
        "expansion": ("top", 2),
    }
    side, offset = band_positions.get((lens or "").strip().lower(), ("bottom", 0))
    if side == "top":
        y2 = max_high - (offset * band_height)
        return y2 - band_height, y2
    y1 = min_low + (offset * band_height)
    return y1, y1 + band_height


def _format_regime_label(block: Mapping[str, Any]) -> str:
    structure = _base_block_label(block)
    volatility = (block.get("volatility_state") or "unknown").title()
    liquidity = (block.get("liquidity_state") or "unknown").title()
    expansion = (block.get("expansion_state") or "unknown").title()
    confidence = block.get("avg_structure_confidence") or block.get("avg_confidence")
    confidence_label = f" (conf {float(confidence):.2f})" if isinstance(confidence, (int, float)) else ""
    return f"{structure} • {volatility} Vol • {liquidity} • {expansion}{confidence_label}"


def detect_regime_changes(blocks: Sequence[Mapping[str, Any]]) -> List[int]:
    changes: List[int] = []
    for block in list(blocks)[1:]:
        epoch = _to_epoch(block.get("known_at") or block.get("start_ts"))
        if epoch is not None:
            changes.append(int(epoch))
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
        lookup_time = _to_naive(candle.time)
        regime = regime_rows.get(lookup_time) if lookup_time else None
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
        context_state = (structure or {}).get("context_regime_state")
        context_direction = (structure or {}).get("context_regime_direction")
        if not isinstance(context_state, str) or not context_state.strip():
            raise RuntimeError(
                "regime_overlay_projection_failed: context_regime_state missing from regime row "
                f"time={lookup_time.isoformat() if isinstance(lookup_time, datetime) else lookup_time}"
            )
        points.append(
            {
                "time": int(epoch),
                "candle_time": candle.time,
                "high": float(candle.high),
                "low": float(candle.low),
                "close": float(candle.close),
                "structure": structure,
                "structure_state": context_state,
                "trend_direction": (str(context_direction or "neutral").strip().lower() or "neutral"),
                "structure_confidence": (structure or {}).get("confidence"),
                "score_margin": None,
                "trust_score": (structure or {}).get("context_trust_score"),
                "is_known": (structure or {}).get("context_is_known"),
                "is_mature": (structure or {}).get("context_is_mature"),
                "is_trustworthy": (structure or {}).get("context_is_trustworthy"),
                "recent_switch_count": (structure or {}).get("context_recent_switch_count"),
                "expansion": expansion,
                "expansion_state": (expansion or {}).get("state"),
                "liquidity": liquidity,
                "liquidity_state": (liquidity or {}).get("state"),
                "volatility": volatility,
                "volatility_state": (volatility or {}).get("state"),
                "confidence": last_regime.get("confidence") if isinstance(last_regime, Mapping) else None,
                "regime_block_id": None,
                "regime_key": last_regime.get("regime_key") if isinstance(last_regime, Mapping) else None,
            }
        )
    return points, min_low, max_high


def _build_structure_block_points(points: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {
            "time": entry.get("candle_time"),
            "structure_state": entry.get("structure_state"),
            "trend_direction": entry.get("trend_direction"),
            "volatility_state": entry.get("volatility_state"),
            "liquidity_state": entry.get("liquidity_state"),
            "expansion_state": entry.get("expansion_state"),
            "confidence": entry.get("confidence"),
            "structure_confidence": entry.get("structure_confidence"),
            "score_margin": entry.get("score_margin"),
            "trust_score": entry.get("trust_score"),
            "is_known": entry.get("is_known"),
            "is_mature": entry.get("is_mature"),
            "is_trustworthy": entry.get("is_trustworthy"),
            "recent_switch_count": entry.get("recent_switch_count"),
        }
        for entry in points
        if isinstance(entry.get("candle_time"), datetime)
    ]


def _attach_block_ids(
    points: Sequence[Mapping[str, Any]],
    block_ids: Mapping[int, str],
) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for idx, entry in enumerate(points):
        item = dict(entry)
        item["regime_block_id"] = block_ids.get(idx)
        enriched.append(item)
    return enriched


def _build_block_price_envelopes(
    points: Sequence[Mapping[str, Any]],
) -> Dict[str, Dict[str, float]]:
    envelopes: Dict[str, Dict[str, float]] = {}
    for entry in points:
        block_id = str(entry.get("regime_block_id") or "").strip()
        if not block_id:
            continue
        low = entry.get("low")
        high = entry.get("high")
        if not isinstance(low, (int, float)) or not isinstance(high, (int, float)):
            continue
        row = envelopes.setdefault(
            block_id,
            {"low": float(low), "high": float(high)},
        )
        row["low"] = min(row["low"], float(low))
        row["high"] = max(row["high"], float(high))
    return envelopes


def _time_to_epoch(value: Any) -> Optional[int]:
    epoch = _to_epoch(value)
    return int(epoch) if epoch is not None else None


def _midpoint_epoch(start_epoch: int, end_epoch: int, known_at_epoch: Optional[int]) -> int:
    label_start = max(start_epoch, int(known_at_epoch)) if known_at_epoch is not None else start_epoch
    return int((label_start + end_epoch) / 2)


def _base_block_label(block: Mapping[str, Any]) -> str:
    state = str(block.get("structure_state") or "").strip().lower()
    labels = {
        "trend_up": "Trend Up",
        "trend_down": "Trend Down",
        "range": "Range",
        "transition_up": "Transition Up",
        "transition_down": "Transition Down",
        "transition_neutral": "Transition",
        "trend": "Trend",
        "transition": "Transition",
    }
    if state in labels:
        return labels[state]
    return state.title() if state else "Unknown"


def _block_label_text(block: Mapping[str, Any], *, tier: str) -> str:
    label = _base_block_label(block)
    if tier != "full":
        return label
    confidence = block.get("avg_structure_confidence") or block.get("avg_confidence")
    trust_score = block.get("avg_trust_score")
    confidence_label = f" ({round(float(confidence) * 100)}%)" if isinstance(confidence, (int, float)) else ""
    trust_label = f" trust {float(trust_score):.2f}" if isinstance(trust_score, (int, float)) else ""
    return f"{label}{confidence_label}{trust_label}"


def _label_tier_for_block(
    block: Mapping[str, Any],
    *,
    cfg: RegimeBlockConfig,
) -> str:
    bars = int(block.get("bars") or 0)
    trust_score = block.get("avg_trust_score")
    if (
        bars >= int(cfg.label_full_bars)
        and bool(block.get("is_mature"))
        and (not isinstance(trust_score, (int, float)) or float(trust_score) >= cfg.label_min_trust)
    ):
        return "full"
    if bars < int(cfg.label_compact_bars):
        return "short"
    return "compact"


def build_regime_markers(
    blocks: Sequence[Mapping[str, Any]],
    candles: Sequence[Candle],
) -> List[Dict[str, Any]]:
    cfg = default_regime_runtime_config().blocks
    price_by_time = {
        normalize_epoch(candle.time): candle.close
        for candle in candles
        if normalize_epoch(candle.time) is not None
    }
    markers: List[Dict[str, Any]] = []
    for block in blocks:
        known_at_epoch = _time_to_epoch(block.get("known_at"))
        start_epoch = _time_to_epoch(block.get("start_ts"))
        end_epoch = _time_to_epoch(block.get("end_ts"))
        if start_epoch is None or end_epoch is None:
            continue
        state = (block.get("structure_state") or "").strip().lower()
        trend_direction = str(block.get("trend_direction") or "neutral")
        color = state_color(state, trend_direction=trend_direction)
        if known_at_epoch is not None:
            known_price = price_by_time.get(known_at_epoch)
            if known_price is not None:
                markers.append(
                    {
                        "time": int(known_at_epoch),
                        "price": known_price,
                        "color": color,
                        "shape": "square",
                        "size": 5,
                        "position": "aboveBar",
                        "subtype": "regime_known_at",
                    }
                )
        label_tier = _label_tier_for_block(block, cfg=cfg)
        label_epoch = _midpoint_epoch(start_epoch, end_epoch, known_at_epoch)
        price_low = block.get("price_low")
        price_high = block.get("price_high")
        if isinstance(price_low, (int, float)) and isinstance(price_high, (int, float)):
            block_span = max(float(price_high) - float(price_low), 1e-6)
            label_price = float(price_high) - (block_span * 0.08)
        else:
            label_price = None
            fallback_epochs = [epoch for epoch in (known_at_epoch, start_epoch, end_epoch) if epoch is not None]
            for fallback_epoch in fallback_epochs:
                candidate = price_by_time.get(fallback_epoch)
                if isinstance(candidate, (int, float)):
                    label_price = float(candidate)
                    break
        if not isinstance(label_price, (int, float)):
            continue
        markers.append(
            {
                "time": int(label_epoch),
                "price": float(label_price),
                "color": color,
                "shape": "circle",
                "size": 4,
                "text": _block_label_text(block, tier=label_tier),
                "position": "aboveBar",
                "subtype": "regime_block_label",
            }
        )
    logger.debug("regime_markers_built | blocks=%s | markers=%s", len(blocks), len(markers))
    return markers


def build_regime_marker_overlay(blocks: Sequence[Mapping[str, Any]], candles: Sequence[Candle]) -> Optional[Dict[str, Any]]:
    markers = build_regime_markers(blocks, candles)
    if not markers:
        return None
    return build_overlay("regime_markers", {"markers": markers})


def _build_boundary_segments(
    blocks: Sequence[Mapping[str, Any]],
    *,
    min_low: float,
    max_high: float,
) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    for idx, block in enumerate(list(blocks)[1:], start=1):
        epoch = _to_epoch(block.get("start_ts"))
        if epoch is None:
            continue
        state = (block.get("structure_state") or "").strip().lower()
        trend_direction = str(block.get("trend_direction") or "neutral")
        segments.append(
            {
                "x1": int(epoch),
                "x2": int(epoch),
                "y1": min_low,
                "y2": max_high,
                "color": _to_rgba(
                    state_color(state, trend_direction=trend_direction),
                    0.34 if idx > 0 else 0.22,
                ),
                "lineWidth": 1,
                "lineStyle": 2,
                "role": "boundary",
            }
        )
    return segments


def _build_regime_payload(
    points: Sequence[Mapping[str, Any]],
    *,
    min_low: float,
    max_high: float,
    timeframe_seconds: int,
    regime_version: Optional[str],
    include_change_markers: bool,
    include_regime_blocks: bool,
    include_regime_points: bool,
) -> Dict[str, Any]:
    regime_config = default_regime_runtime_config()
    block_points = _build_structure_block_points(points)
    blocks, block_ids = build_regime_blocks(
        block_points,
        timeframe_seconds=timeframe_seconds,
        config=regime_config.blocks,
    )
    points_with_blocks = _attach_block_ids(points, block_ids)
    price_envelopes = _build_block_price_envelopes(points_with_blocks)
    boxes: List[Dict[str, Any]] = []
    regime_blocks: List[Dict[str, Any]] = []
    for block in blocks:
        start_epoch = _to_epoch(block.get("start_ts"))
        end_epoch = _to_epoch(block.get("end_ts"))
        known_at_epoch = _to_epoch(block.get("known_at"))
        if start_epoch is None or end_epoch is None:
            continue
        state = str(block.get("structure_state") or "")
        trend_direction = str(block.get("trend_direction") or "neutral")
        block_id = str(block.get("block_id") or "")
        envelope = price_envelopes.get(block_id) or {}
        regime_blocks.append(
            {
                "x1": int(start_epoch),
                "x2": int(end_epoch) + timeframe_seconds,
                "known_at": int(known_at_epoch) if known_at_epoch is not None else int(start_epoch),
                "structure": {"state": state, "trend_direction": trend_direction},
                "volatility": {"state": block.get("volatility_state")},
                "liquidity": {"state": block.get("liquidity_state")},
                "expansion": {"state": block.get("expansion_state")},
                "confidence": block.get("avg_structure_confidence") or block.get("avg_confidence"),
                "entry_confidence": block.get("entry_confidence"),
                "score_margin": block.get("avg_score_margin"),
                "trust_score": block.get("avg_trust_score"),
                "bars": block.get("bars"),
                "regime_key": block.get("regime_key"),
                "block_id": block_id,
                "price_low": envelope.get("low"),
                "price_high": envelope.get("high"),
                "is_known": block.get("is_known"),
                "is_mature": block.get("is_mature"),
                "is_trustworthy": block.get("is_trustworthy"),
                "recent_switch_count": block.get("recent_switch_count"),
            }
        )
        base_color = state_color(state, trend_direction=trend_direction)
        opacity_source = block.get("avg_trust_score")
        if not isinstance(opacity_source, (int, float)):
            opacity_source = block.get("avg_structure_confidence")
        opacity = confidence_to_opacity(opacity_source)
        y1 = min_low
        y2 = max_high
        if state.strip().lower().startswith("transition"):
            envelope_low = float(envelope.get("low", min_low))
            envelope_high = float(envelope.get("high", max_high))
            block_span = max(envelope_high - envelope_low, 1e-6)
            padding = max(block_span * 0.18, (max_high - min_low) * 0.01)
            y1 = max(min_low, envelope_low - padding)
            y2 = min(max_high, envelope_high + padding)
        boxes.append(
            {
                "x1": int(start_epoch),
                "x2": int(end_epoch) + timeframe_seconds,
                "y1": y1,
                "y2": y2,
                "color": _to_rgba(base_color, opacity),
                "border": {"color": _to_rgba(base_color, min(opacity + 0.12, 0.4)), "width": 1},
                "precision": 4,
                "known_at": int(known_at_epoch) if known_at_epoch is not None else int(start_epoch),
                "state": state,
                "trend_direction": trend_direction,
                "confidence": block.get("avg_structure_confidence") or block.get("avg_confidence"),
                "score_margin": block.get("avg_score_margin"),
                "trust_score": block.get("avg_trust_score"),
                "regime_key": block.get("regime_key"),
                "block_id": block_id,
                "is_mature": block.get("is_mature"),
                "is_trustworthy": block.get("is_trustworthy"),
            }
        )

    segments: List[Dict[str, Any]] = []
    if include_change_markers:
        segments.extend(_build_boundary_segments(blocks, min_low=min_low, max_high=max_high))

    current_block = blocks[-1] if blocks else None
    regime_label = _format_regime_label(current_block) if current_block else None
    payload: Dict[str, Any] = {
        "boxes": boxes,
        "segments": segments,
        "summary": {
            "regime_version": regime_version,
            "points": len(points_with_blocks),
            "changes": max(len(blocks) - 1, 0),
            "blocks": len(boxes),
            "current_regime_label": regime_label,
        },
    }
    if include_regime_blocks:
        payload["regime_blocks"] = regime_blocks
    if include_regime_points:
        payload["regime_points"] = list(points_with_blocks)
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
        if not state or not isinstance(candle_time, datetime):
            continue
        block_points.append(
            {
                "time": candle_time,
                "structure_state": state,
                "trend_direction": "neutral",
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
    include_regime_blocks: bool = True,
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
        include_regime_blocks=include_regime_blocks,
        include_regime_points=include_regime_points,
    )
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
    block_points = _build_structure_block_points(points)
    blocks, block_ids = build_regime_blocks(
        block_points,
        timeframe_seconds=timeframe_seconds,
        config=default_regime_runtime_config().blocks,
    )
    points_with_blocks = _attach_block_ids(points, block_ids)
    payload = _build_regime_payload(
        points_with_blocks,
        min_low=min_low,
        max_high=max_high,
        timeframe_seconds=timeframe_seconds,
        regime_version=regime_version,
        include_change_markers=include_change_markers,
        include_regime_blocks=True,
        include_regime_points=False,
    )
    if include_change_markers:
        change_epochs = detect_regime_changes(blocks)
        logger.debug(
            "regime_overlay_change_markers | points=%s | changes=%s | epochs=%s",
            len(points_with_blocks),
            len(change_epochs),
            change_epochs[:12],
        )
    overlays = [build_overlay("regime_overlay", payload)]
    for lens in ("expansion", "liquidity", "volatility"):
        boxes = _build_lens_boxes(
            points=points_with_blocks,
            lens=lens,
            min_low=min_low,
            max_high=max_high,
            timeframe_seconds=timeframe_seconds,
        )
        if not boxes:
            continue
        overlays.append(
            build_overlay(
                f"regime_overlay_{lens}",
                {
                    "boxes": boxes,
                    "summary": {
                        "regime_version": regime_version,
                        "lens": lens,
                        "segments": 0,
                        "boxes": len(boxes),
                    },
                },
            )
        )
    if include_marker_overlay:
        marker_overlay = build_regime_marker_overlay(blocks, candles)
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
