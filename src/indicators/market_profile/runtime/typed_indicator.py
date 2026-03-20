"""Authoritative walk-forward market profile runtime indicator."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import Indicator, RuntimeOverlay, RuntimeOutput
from indicators.manifest import build_runtime_spec
from signals.overlays.registry import register_overlay_type
from signals.overlays.schema import build_overlay

from ..compute.internal.runtime_profiles import profile_identity, resolve_effective_profiles
from ..manifest import MANIFEST


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


class TypedMarketProfileIndicator(Indicator):
    def __init__(
        self,
        *,
        indicator_id: str,
        version: str,
        params: Mapping[str, Any],
        source_facts: Mapping[str, Any],
    ) -> None:
        self.runtime_spec = build_runtime_spec(
            MANIFEST,
            instance_id=indicator_id,
            version=version,
        )
        self._indicator_id = str(indicator_id or "").strip()
        self._bin_size = _as_float(params.get("bin_size"), "bin_size")
        self._price_precision = _as_int(params.get("price_precision"), "price_precision")
        self._profile_params = dict(source_facts.get("profile_params") or {})
        self._profiles_payload = list(source_facts.get("profiles") or [])
        self._symbol = str(source_facts.get("symbol") or "")
        self._extend_to_end = bool(self._profile_params.get("extend_value_area_to_chart_end"))
        self._previous_profile_key: str | None = None
        self._previous_location: str | None = None
        self._current_bar_time = datetime.min
        self._current_effective_profiles: list[Any] = []
        self._current_transform_summary: dict[str, Any] = {}
        self._current_overlay_summary: dict[str, Any] = {}
        self._overlay_ready = False
        self._outputs: dict[str, RuntimeOutput] = {
            "value_area_metrics": RuntimeOutput(bar_time=datetime.min, ready=False, value={}),
            "value_location": RuntimeOutput(bar_time=datetime.min, ready=False, value={}),
            "balance_state": RuntimeOutput(bar_time=datetime.min, ready=False, value={}),
            "balance_breakout": RuntimeOutput(bar_time=datetime.min, ready=False, value={}),
        }
        self._overlays: dict[str, RuntimeOverlay] = {
            "value_area": RuntimeOverlay(bar_time=datetime.min, ready=False, value={}),
        }

    def apply_bar(self, bar: Any, inputs: Mapping[Any, RuntimeOutput]) -> None:
        if not isinstance(bar, Candle):
            raise RuntimeError("market_profile_apply_failed: Candle input required")
        if inputs:
            raise RuntimeError("market_profile_apply_failed: market_profile has no dependencies")

        self._current_bar_time = bar.time
        current_epoch = int(bar.time.timestamp())
        effective_profiles, transform_summary = resolve_effective_profiles(
            profiles_payload=self._profiles_payload,
            profile_params=self._profile_params,
            current_epoch=current_epoch,
            symbol=self._symbol or None,
        )
        if not effective_profiles:
            self._current_effective_profiles = []
            self._current_transform_summary = dict(transform_summary or {})
            self._current_overlay_summary = {}
            self._overlay_ready = False
            not_ready = RuntimeOutput(bar_time=bar.time, ready=False, value={})
            self._outputs = {
                "value_area_metrics": not_ready,
                "value_location": not_ready,
                "balance_state": not_ready,
                "balance_breakout": not_ready,
            }
            self._previous_profile_key = None
            self._previous_location = None
            return

        active_profile = effective_profiles[-1]
        active_profile_key = profile_identity(active_profile)
        if self._previous_profile_key != active_profile_key:
            self._previous_location = None
        self._previous_profile_key = active_profile_key

        close = float(bar.close)
        val = float(active_profile.val)
        vah = float(active_profile.vah)
        poc = float(active_profile.poc)
        location = "inside_value"
        if close > vah:
            location = "above_value"
        elif close < val:
            location = "below_value"
        balance_state = "balanced" if location == "inside_value" else "imbalanced"
        events: list[dict[str, str]] = []
        if self._previous_location == "inside_value" and location == "above_value":
            events.append({"key": "balance_breakout_long"})
        elif self._previous_location == "inside_value" and location == "below_value":
            events.append({"key": "balance_breakout_short"})
        self._previous_location = location
        self._current_effective_profiles = list(effective_profiles)
        self._current_transform_summary = dict(transform_summary or {})
        self._current_overlay_summary = {
            "location": location,
            "balance_state": balance_state,
            "active_profile_key": active_profile_key,
        }
        self._overlay_ready = True

        self._outputs = {
            "value_area_metrics": RuntimeOutput(
                bar_time=bar.time,
                ready=True,
                value={
                    "poc": poc,
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

    def _build_overlay(
        self,
        *,
        bar_time: datetime,
        current_epoch: int,
    ) -> dict[str, Any]:
        boxes = []
        for profile in self._current_effective_profiles:
            start_epoch = int(profile.start.timestamp())
            end_epoch = int(profile.end.timestamp())
            if end_epoch > current_epoch:
                continue
            box_end = int(current_epoch) if self._extend_to_end else end_epoch
            if box_end < start_epoch:
                continue
            boxes.append(
                {
                    "x1": start_epoch,
                    "x2": box_end,
                    "y1": float(profile.val),
                    "y2": float(profile.vah),
                    "profile_key": profile_identity(profile),
                    "fillColor": "rgba(59, 130, 246, 0.1)",
                    "borderColor": "#3b82f6",
                    "borderWidth": 1,
                    "borderStyle": 2,
                }
            )
        payload = {
            "boxes": boxes,
            "markers": [],
            "bubbles": [],
            "summary": {
                **dict(self._current_overlay_summary or {}),
                "transform_summary": dict(self._current_transform_summary or {}),
            },
        }
        overlay = dict(build_overlay("market_profile", payload))
        overlay["indicator_id"] = self._indicator_id
        if self._symbol:
            overlay["symbol"] = self._symbol
        overlay["known_at"] = int(bar_time.timestamp())
        return overlay

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return dict(self._outputs)

    def overlay_snapshot(self) -> Mapping[str, RuntimeOverlay]:
        if not self._overlay_ready:
            return {
                "value_area": RuntimeOverlay(
                    bar_time=self._current_bar_time,
                    ready=False,
                    value=self._build_overlay(
                        bar_time=self._current_bar_time,
                        current_epoch=int(self._current_bar_time.timestamp()),
                    ),
                )
            }
        return {
            "value_area": RuntimeOverlay(
                bar_time=self._current_bar_time,
                ready=True,
                value=self._build_overlay(
                    bar_time=self._current_bar_time,
                    current_epoch=int(self._current_bar_time.timestamp()),
                ),
            )
        }


__all__ = ["TypedMarketProfileIndicator"]
