from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Sequence

from engines.bot_runtime.core.indicator_state import ensure_builtin_indicator_plugins_registered
from engines.bot_runtime.core.indicator_state.plugins import plugin_registry
from signals.base import BaseSignal
from signals.engine.signal_generator import build_signal_overlays

from .runtime_projection import build_runtime_state_overlay

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OverlayProjectionContext:
    indicator_id: str
    meta: Mapping[str, Any]
    df: Any
    symbol: str
    timeframe: str
    signals: Sequence[BaseSignal] = ()


def project_indicator_overlays(ctx: OverlayProjectionContext) -> List[Mapping[str, Any]]:
    indicator_type = str(ctx.meta.get("type") or "").strip().lower()
    if not indicator_type:
        raise RuntimeError("overlay_projection_failed: missing indicator_type")

    ensure_builtin_indicator_plugins_registered()
    try:
        manifest = plugin_registry().resolve(indicator_type)
    except Exception as exc:
        raise RuntimeError(
            f"overlay_projection_failed: plugin_missing indicator_type={indicator_type}"
        ) from exc

    generated: List[Mapping[str, Any]] = []

    state_overlays = _project_state_overlay(ctx)
    if state_overlays:
        generated.extend(state_overlays)

    signal_overlays = _project_signal_overlay(
        indicator_type=indicator_type,
        signals=ctx.signals,
        df=ctx.df,
    )
    if signal_overlays:
        generated.extend(signal_overlays)

    merged = _merge_overlay_sets(generated)
    logger.info(
        "event=overlay_projection_composed indicator_id=%s indicator_type=%s has_state_projector=%s has_signal_adapter=%s overlays_in=%s overlays_out=%s",
        ctx.indicator_id,
        indicator_type,
        bool(getattr(manifest, "overlay_projector", None)),
        bool(getattr(manifest, "signal_overlay_adapter", None)),
        len(generated),
        len(merged),
    )
    return merged


def _project_state_overlay(ctx: OverlayProjectionContext) -> List[Mapping[str, Any]]:
    try:
        overlay = build_runtime_state_overlay(
            indicator_id=ctx.indicator_id,
            meta=ctx.meta,
            df=ctx.df,
            symbol=ctx.symbol,
            timeframe=ctx.timeframe,
        )
    except LookupError:
        return []
    if not isinstance(overlay, Mapping):
        return []
    return [dict(overlay)]


def _project_signal_overlay(
    *,
    indicator_type: str,
    signals: Sequence[BaseSignal],
    df: Any,
) -> List[Mapping[str, Any]]:
    overlays = build_signal_overlays(indicator_type, list(signals), df)
    return [dict(item) for item in overlays if isinstance(item, Mapping)]


def _merge_overlay_sets(overlays: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    merged: List[Mapping[str, Any]] = []
    index_by_type: Dict[str, int] = {}
    for raw in overlays:
        if not isinstance(raw, Mapping):
            continue
        overlay = dict(raw)
        overlay_type = str(overlay.get("type") or "").strip().lower()
        if not overlay_type:
            merged.append(overlay)
            continue
        existing_index = index_by_type.get(overlay_type)
        if existing_index is None:
            index_by_type[overlay_type] = len(merged)
            merged.append(overlay)
            continue
        merged[existing_index] = _merge_overlay_entry(merged[existing_index], overlay)
    return _sort_overlays_deterministically(merged)


def _merge_overlay_entry(base_entry: Mapping[str, Any], extra_entry: Mapping[str, Any]) -> Mapping[str, Any]:
    merged_entry = dict(base_entry)
    base_payload = base_entry.get("payload")
    extra_payload = extra_entry.get("payload")
    if isinstance(base_payload, Mapping) and isinstance(extra_payload, Mapping):
        merged_entry["payload"] = _merge_overlay_payload(base_payload, extra_payload)
    elif isinstance(extra_payload, Mapping):
        merged_entry["payload"] = dict(extra_payload)
    for key in ("pane_views", "renderers", "ui"):
        if key not in merged_entry and key in extra_entry:
            merged_entry[key] = extra_entry.get(key)
    return merged_entry


def _merge_overlay_payload(base_payload: Mapping[str, Any], extra_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    merged_payload = dict(base_payload)
    collection_keys = (
        "markers",
        "bubbles",
        "price_lines",
        "polylines",
        "boxes",
        "segments",
        "touch_points",
    )
    for key in collection_keys:
        base_items = base_payload.get(key)
        extra_items = extra_payload.get(key)
        base_list = [dict(item) for item in base_items] if isinstance(base_items, list) else []
        extra_list = [dict(item) for item in extra_items] if isinstance(extra_items, list) else []
        if base_list or extra_list:
            merged_payload[key] = base_list + extra_list
    for key, value in extra_payload.items():
        if key in collection_keys:
            continue
        if key not in merged_payload:
            merged_payload[key] = value
    return merged_payload


def _sort_overlays_deterministically(overlays: Sequence[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    def _sort_key_mapping(item: Mapping[str, Any]) -> tuple[Any, ...]:
        return (str(item.get("type") or ""), str(item.get("id") or ""))

    def _sort_marker(item: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            int(item.get("time") or 0),
            float(item.get("price") or 0.0),
            str(item.get("shape") or ""),
            str(item.get("text") or ""),
        )

    def _sort_bubble(item: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            int(item.get("time") or 0),
            float(item.get("price") or 0.0),
            str(item.get("label") or ""),
            str(item.get("detail") or ""),
        )

    def _sort_price_line(item: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            float(item.get("price") or 0.0),
            int(item.get("originTime") or 0),
            int(item.get("endTime") or 0),
            str(item.get("title") or ""),
        )

    normalized: List[Mapping[str, Any]] = []
    for raw in overlays:
        if not isinstance(raw, Mapping):
            continue
        entry = dict(raw)
        payload = entry.get("payload")
        if isinstance(payload, Mapping):
            payload_copy = dict(payload)
            markers = payload_copy.get("markers")
            if isinstance(markers, list):
                payload_copy["markers"] = sorted(
                    [dict(m) for m in markers if isinstance(m, Mapping)],
                    key=_sort_marker,
                )
            bubbles = payload_copy.get("bubbles")
            if isinstance(bubbles, list):
                payload_copy["bubbles"] = sorted(
                    [dict(b) for b in bubbles if isinstance(b, Mapping)],
                    key=_sort_bubble,
                )
            price_lines = payload_copy.get("price_lines")
            if isinstance(price_lines, list):
                payload_copy["price_lines"] = sorted(
                    [dict(line) for line in price_lines if isinstance(line, Mapping)],
                    key=_sort_price_line,
                )
            entry["payload"] = payload_copy
        normalized.append(entry)
    return sorted(normalized, key=_sort_key_mapping)


__all__ = [
    "OverlayProjectionContext",
    "project_indicator_overlays",
]
