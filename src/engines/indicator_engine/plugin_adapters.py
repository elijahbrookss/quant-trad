"""Indicator-agnostic plugin adapter utilities."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping

from engines.bot_runtime.core.domain import Candle

from .contracts import OverlayProjectionInput


def generic_overlay_entries(projection_input: OverlayProjectionInput) -> Mapping[str, Mapping[str, Any]]:
    payload = projection_input.snapshot.payload or {}
    overlays = payload.get("overlays") if isinstance(payload, Mapping) else None
    if not isinstance(overlays, list):
        return {}
    entries: Dict[str, Mapping[str, Any]] = {}
    for idx, overlay in enumerate(overlays):
        if not isinstance(overlay, Mapping):
            continue
        key = str(overlay.get("id") or f"overlay:{idx}")
        entries[key] = dict(overlay)
    return entries


def generic_rule_payload(
    *,
    snapshot_payload: Mapping[str, Any],
    candle: Candle,
    previous_candle: Candle | None,
) -> Dict[str, Any]:
    raw_signals = snapshot_payload.get("signals") if isinstance(snapshot_payload, Mapping) else None
    signals: List[Dict[str, Any]] = []
    if isinstance(raw_signals, list):
        for signal in raw_signals:
            if isinstance(signal, Mapping):
                signals.append(dict(signal))
    return {"signals": signals}
