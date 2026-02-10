from __future__ import annotations

import logging
from datetime import timezone
from typing import Any, Dict, Mapping, Optional

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.indicator_state import ensure_builtin_indicator_plugins_registered
from engines.bot_runtime.core.indicator_state.contracts import OverlayProjectionInput
from engines.bot_runtime.core.indicator_state.plugins import plugin_registry
from signals.overlays.schema import normalize_overlays

logger = logging.getLogger(__name__)


def build_runtime_state_overlay(
    *,
    indicator_id: str,
    meta: Mapping[str, Any],
    df: Any,
    symbol: str,
    timeframe: str,
    overlay_options: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    if not _runtime_projection_enabled(overlay_options):
        return None

    indicator_type = str(meta.get("type") or "").strip().lower()
    if not indicator_type:
        return None

    ensure_builtin_indicator_plugins_registered()
    try:
        plugin = plugin_registry().resolve(indicator_type)
    except Exception:
        return None

    if plugin.overlay_projector is None:
        return None

    candles = _candles_from_frame(df)
    if not candles:
        raise LookupError("No overlays computed for given window")

    engine = plugin.engine_factory(meta)
    state = engine.initialize(
        {
            "symbol": symbol,
            "timeframe": timeframe,
            "indicator_id": indicator_id,
        }
    )
    for candle in candles:
        engine.apply_bar(state, candle)
    snapshot = engine.snapshot(state)
    entries = plugin.overlay_projector(
        OverlayProjectionInput(
            snapshot=snapshot,
            previous_projection_state={"seq": 0, "revision": -1, "entries": {}},
        )
    )
    if not isinstance(entries, Mapping) or not entries:
        raise LookupError("No overlays computed for given window")

    normalized_count = 0
    for entry in entries.values():
        if not isinstance(entry, Mapping):
            continue
        normalized = normalize_overlays(indicator_type, [dict(entry)])
        if not normalized:
            continue
        normalized_count += 1
        payload = normalized[0].get("payload") if isinstance(normalized[0], Mapping) else None
        logger.info(
            "event=overlay_runtime_state_projected indicator_id=%s indicator_type=%s entries=%s payload_keys=%s",
            indicator_id,
            indicator_type,
            len(entries),
            list((payload or {}).keys()) if isinstance(payload, Mapping) else [],
        )
        return dict(normalized[0])
    if normalized_count == 0:
        raise LookupError("No overlays computed for given window")
    return None


def _runtime_projection_enabled(overlay_options: Mapping[str, Any]) -> bool:
    mode = overlay_options.get("projection_mode")
    if isinstance(mode, str):
        text = mode.strip().lower()
        if text in {"legacy", "indicator"}:
            return False
        if text in {"runtime_state", "state"}:
            return True
    explicit = overlay_options.get("runtime_projection")
    if explicit is not None:
        return bool(explicit)
    # Default to runtime-state projection for plugin-backed indicators.
    return True


def _candles_from_frame(df: Any) -> list[Candle]:
    candles: list[Candle] = []
    for timestamp, row in df.iterrows():
        dt = timestamp.to_pydatetime() if hasattr(timestamp, "to_pydatetime") else timestamp
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        candles.append(
            Candle(
                time=dt,
                open=float(row.get("open")),
                high=float(row.get("high")),
                low=float(row.get("low")),
                close=float(row.get("close")),
                volume=float(row.get("volume")) if row.get("volume") is not None else None,
            )
        )
    return candles

