"""Authoritative walk-forward market profile runtime indicator."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import Indicator, RuntimeOverlay, RuntimeOutput
from indicators.manifest import build_runtime_spec
from overlays.registry import register_overlay_type
from overlays.schema import build_overlay

from ..compute.internal.runtime_profiles import IncrementalRuntimeProfileResolver, profile_identity
from ..manifest import MANIFEST
from .outputs import build_market_profile_outputs, build_not_ready_outputs
from .state import derive_market_profile_bar_state


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
        self._profile_resolver = IncrementalRuntimeProfileResolver(
            profiles_payload=self._profiles_payload,
            profile_params=self._profile_params,
            symbol=self._symbol or None,
        )
        self._previous_profile_key: str | None = None
        self._previous_location: str | None = None
        self._current_bar_time = datetime.min
        self._current_effective_profiles: list[Any] = []
        self._current_transform_summary: dict[str, Any] = {}
        self._current_overlay_summary: dict[str, Any] = {}
        self._overlay_ready = False
        self._outputs: dict[str, RuntimeOutput] = build_not_ready_outputs(datetime.min)

    def apply_bar(self, bar: Any, inputs: Mapping[Any, RuntimeOutput]) -> None:
        if not isinstance(bar, Candle):
            raise RuntimeError("market_profile_apply_failed: Candle input required")
        if inputs:
            raise RuntimeError("market_profile_apply_failed: market_profile has no dependencies")

        self._current_bar_time = bar.time
        current_epoch = int(bar.time.timestamp())
        effective_profiles, transform_summary = self._profile_resolver.resolve(
            current_epoch=current_epoch,
        )
        self._current_transform_summary = dict(transform_summary or {})
        if not effective_profiles:
            self._reset_outputs(bar.time)
            return

        active_profile = effective_profiles[-1]
        bar_state = derive_market_profile_bar_state(
            bar=bar,
            active_profile=active_profile,
            previous_profile_key=self._previous_profile_key,
            previous_location=self._previous_location,
        )
        self._previous_profile_key = bar_state.active_profile_key
        self._previous_location = bar_state.location
        self._current_effective_profiles = list(effective_profiles)
        self._current_overlay_summary = {
            "location": bar_state.location,
            "balance_state": bar_state.balance_state,
            "active_profile_key": bar_state.active_profile_key,
        }
        self._overlay_ready = True
        self._outputs = build_market_profile_outputs(bar_state)

    def _reset_outputs(self, bar_time: datetime) -> None:
        self._current_effective_profiles = []
        self._current_overlay_summary = {}
        self._overlay_ready = False
        self._outputs = build_not_ready_outputs(bar_time)
        self._previous_profile_key = None
        self._previous_location = None

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
