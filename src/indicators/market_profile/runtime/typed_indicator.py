"""Native market profile runtime indicator."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Mapping

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import (
    Indicator,
    IndicatorManifest,
    OverlayDefinition,
    OutputDefinition,
    RuntimeOverlay,
    RuntimeOutput,
)
from signals.overlays.registry import register_overlay_type
from signals.overlays.schema import build_overlay


register_overlay_type(
    "market_profile",
    label="Market Profile",
    pane_views=("va_box", "touch"),
    description="Market profile value area boxes and touch markers.",
    renderers={"lightweight": "va_box", "mpl": "box"},
    payload_keys=("boxes", "markers", "bubbles"),
    ui_color="#38bdf8",
)


def _as_float(value: Any, field: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"market_profile_config_invalid: {field} must be numeric") from exc


def _as_int(value: Any, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"market_profile_config_invalid: {field} must be int") from exc


def _apply_row_to_histogram(
    *,
    histogram: dict[float, int],
    low: float,
    high: float,
    bin_size: float,
    price_precision: int,
) -> None:
    if not math.isfinite(low) or not math.isfinite(high):
        return
    if high < low:
        low, high = high, low
    span = max(high - low, 0.0)
    steps = int(math.floor(span / bin_size + 1e-9))
    for index in range(steps + 1):
        price = low + index * bin_size
        bucket = round(round(price / bin_size) * bin_size, price_precision)
        histogram[bucket] = int(histogram.get(bucket, 0)) + 1


def _extract_value_area(histogram: Mapping[float, int], price_precision: int) -> dict[str, float] | None:
    total = sum(int(count) for count in histogram.values())
    if total <= 0:
        return None
    ordered = sorted(histogram.items(), key=lambda item: item[1], reverse=True)
    cumulative = 0
    prices: list[float] = []
    for price, count in ordered:
        cumulative += int(count)
        prices.append(float(price))
        if cumulative >= total * 0.7:
            break
    return {
        "poc": round(float(ordered[0][0]), price_precision),
        "vah": round(max(prices), price_precision),
        "val": round(min(prices), price_precision),
    }


class TypedMarketProfileIndicator(Indicator):
    def __init__(self, *, indicator_id: str, version: str, params: Mapping[str, Any]) -> None:
        self.manifest = IndicatorManifest(
            id=indicator_id,
            version=version,
            dependencies=(),
            outputs=(
                OutputDefinition(name="value_area_metrics", type="metric"),
                OutputDefinition(name="value_location", type="context"),
                OutputDefinition(name="balance_state", type="context"),
                OutputDefinition(name="balance_breakout", type="signal"),
            ),
            overlays=(
                OverlayDefinition(name="value_area", overlay_type="market_profile"),
                OverlayDefinition(name="breakout_markers", overlay_type="market_profile"),
            ),
        )
        self._bin_size = _as_float(params.get("bin_size"), "bin_size")
        self._price_precision = _as_int(params.get("price_precision"), "price_precision")
        self._active_session: str | None = None
        self._session_start_epoch: int | None = None
        self._histogram: dict[float, int] = {}
        self._previous_location: str | None = None
        self._breakout_markers: list[dict[str, Any]] = []
        self._outputs: dict[str, RuntimeOutput] = {
            "value_area_metrics": RuntimeOutput(bar_time=datetime.min, ready=False, value={}),
            "value_location": RuntimeOutput(bar_time=datetime.min, ready=False, value={}),
            "balance_state": RuntimeOutput(bar_time=datetime.min, ready=False, value={}),
            "balance_breakout": RuntimeOutput(bar_time=datetime.min, ready=False, value={}),
        }
        self._overlays: dict[str, RuntimeOverlay] = {
            "value_area": RuntimeOverlay(bar_time=datetime.min, ready=False, value={}),
            "breakout_markers": RuntimeOverlay(bar_time=datetime.min, ready=False, value={}),
        }

    def apply_bar(self, bar: Any, inputs: Mapping[Any, RuntimeOutput]) -> None:
        if not isinstance(bar, Candle):
            raise RuntimeError("market_profile_apply_failed: Candle input required")
        if inputs:
            raise RuntimeError("market_profile_apply_failed: market_profile has no dependencies")

        session_key = bar.time.date().isoformat()
        if self._active_session is None:
            self._active_session = session_key
            self._session_start_epoch = int(bar.time.timestamp())
        elif self._active_session != session_key:
            self._active_session = session_key
            self._session_start_epoch = int(bar.time.timestamp())
            self._histogram = {}
            self._previous_location = None
            self._breakout_markers = []

        _apply_row_to_histogram(
            histogram=self._histogram,
            low=float(bar.low),
            high=float(bar.high),
            bin_size=self._bin_size,
            price_precision=self._price_precision,
        )
        value_area = _extract_value_area(self._histogram, self._price_precision)
        if value_area is None:
            not_ready = RuntimeOutput(bar_time=bar.time, ready=False, value={})
            not_ready_overlay = RuntimeOverlay(bar_time=bar.time, ready=False, value={})
            self._outputs = {
                "value_area_metrics": not_ready,
                "value_location": not_ready,
                "balance_state": not_ready,
                "balance_breakout": not_ready,
            }
            self._overlays = {
                "value_area": not_ready_overlay,
                "breakout_markers": not_ready_overlay,
            }
            return

        close = float(bar.close)
        val = float(value_area["val"])
        vah = float(value_area["vah"])
        location = "inside_value"
        if close > vah:
            location = "above_value"
        elif close < val:
            location = "below_value"
        balance_state = "balanced" if location == "inside_value" else "imbalanced"
        events: list[dict[str, str]] = []
        if self._previous_location == "inside_value" and location == "above_value":
            events.append({"key": "balance_breakout_long"})
            self._breakout_markers.append(
                {
                    "time": int(bar.time.timestamp()),
                    "price": close,
                    "position": "aboveBar",
                    "shape": "arrowUp",
                    "color": "#16a34a",
                    "text": "BO-L",
                }
            )
        elif self._previous_location == "inside_value" and location == "below_value":
            events.append({"key": "balance_breakout_short"})
            self._breakout_markers.append(
                {
                    "time": int(bar.time.timestamp()),
                    "price": close,
                    "position": "belowBar",
                    "shape": "arrowDown",
                    "color": "#dc2626",
                    "text": "BO-S",
                }
            )
        self._previous_location = location
        start_epoch = int(self._session_start_epoch or int(bar.time.timestamp()))
        end_epoch = int(bar.time.timestamp())
        color = "#38bdf8" if balance_state == "balanced" else "#f59e0b"
        value_area_overlay = build_overlay(
            "market_profile",
            {
                "boxes": [
                    {
                        "x1": start_epoch,
                        "x2": end_epoch,
                        "y1": val,
                        "y2": vah,
                        "color": "rgba(56,189,248,0.12)" if balance_state == "balanced" else "rgba(245,158,11,0.12)",
                        "border": {"color": color, "width": 1},
                        "precision": self._price_precision,
                    }
                ],
                "markers": [],
                "bubbles": [],
                "summary": {
                    "session": self._active_session,
                    "location": location,
                    "balance_state": balance_state,
                },
            },
        )
        breakout_overlay = build_overlay(
            "market_profile",
            {
                "boxes": [],
                "markers": list(self._breakout_markers),
                "bubbles": [],
                "summary": {"events": len(self._breakout_markers)},
            },
        )
        self._outputs = {
            "value_area_metrics": RuntimeOutput(
                bar_time=bar.time,
                ready=True,
                value={
                    "poc": float(value_area["poc"]),
                    "vah": vah,
                    "val": val,
                    "value_area_width": max(vah - val, 0.0),
                },
            ),
            "value_location": RuntimeOutput(
                bar_time=bar.time,
                ready=True,
                value={"state_key": location},
            ),
            "balance_state": RuntimeOutput(
                bar_time=bar.time,
                ready=True,
                value={"state_key": balance_state},
            ),
            "balance_breakout": RuntimeOutput(
                bar_time=bar.time,
                ready=True,
                value={"events": events},
            ),
        }
        self._overlays = {
            "value_area": RuntimeOverlay(
                bar_time=bar.time,
                ready=True,
                value=dict(value_area_overlay),
            ),
            "breakout_markers": RuntimeOverlay(
                bar_time=bar.time,
                ready=True,
                value=dict(breakout_overlay),
            ),
        }

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return dict(self._outputs)

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        return dict(self._overlays)


__all__ = ["TypedMarketProfileIndicator"]
