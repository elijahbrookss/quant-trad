"""Runtime reporting constants and overlay helpers."""

from __future__ import annotations

from typing import Optional

TRADE_OVERLAY_SOURCE = "trade_levels"
TRADE_STOP_COLOR = "#f87171"
TRADE_TARGET_COLOR = "#22d3ee"
TRADE_RAY_MIN_SECONDS = 900
TRADE_RAY_SPAN_MULTIPLIER = 120


def instrument_key(datasource: Optional[str], exchange: Optional[str], symbol: Optional[str]) -> str:
    return "::".join(
        [
            (datasource or "").strip().lower(),
            (exchange or "").strip().lower(),
            (symbol or "").strip().upper(),
        ]
    )


def payload_has_content(payload: object) -> bool:
    return bool(payload)


def trim_overlay_payload(payload: object, current_epoch: object) -> tuple[object, bool]:
    _ = current_epoch
    return payload, True


__all__ = [
    "TRADE_OVERLAY_SOURCE",
    "TRADE_STOP_COLOR",
    "TRADE_TARGET_COLOR",
    "TRADE_RAY_MIN_SECONDS",
    "TRADE_RAY_SPAN_MULTIPLIER",
    "instrument_key",
    "payload_has_content",
    "trim_overlay_payload",
]
