"""Authoritative walk-forward market profile runtime indicator."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Mapping

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import Indicator, RuntimeOverlay, RuntimeOutput
from indicators.manifest import build_runtime_spec
from overlays.registry import register_overlay_type
from overlays.schema import build_overlay

from ..compute.internal.runtime_profiles import IncrementalRuntimeProfileResolver, profile_identity
from ..manifest import (
    DEFAULT_BREAKOUT_CONFIRM_BARS,
    DEFAULT_RECLAIM_MAX_BARS,
    DEFAULT_RETEST_MAX_BARS,
    DEFAULT_RETEST_ATR_PERIOD,
    DEFAULT_RETEST_HOLD_CONFIRM_BARS,
    DEFAULT_RETEST_MAX_PENETRATION_ATR,
    DEFAULT_RETEST_MIN_ACCEPTANCE_BARS,
    DEFAULT_RETEST_MIN_EXCURSION_ATR,
    DEFAULT_RETEST_TOUCH_TOLERANCE_ATR,
    MANIFEST,
)
from .outputs import build_market_profile_outputs, build_not_ready_outputs
from .signal_state import BreakoutRetestStateMachine
from .state import derive_market_profile_bar_state

log = logging.getLogger(__name__)


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
        source_continuity = source_facts.get("source_candle_continuity")
        self._source_candle_continuity = (
            dict(source_continuity)
            if isinstance(source_continuity, Mapping)
            else {}
        )
        self._source_continuity_warning_logged = False
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
        self._signal_state_params = {
            "breakout_confirm_bars": params.get(
                "breakout_confirm_bars", DEFAULT_BREAKOUT_CONFIRM_BARS
            ),
            "reclaim_max_bars": params.get(
                "reclaim_max_bars", DEFAULT_RECLAIM_MAX_BARS
            ),
            "retest_min_acceptance_bars": params.get(
                "retest_min_acceptance_bars", DEFAULT_RETEST_MIN_ACCEPTANCE_BARS
            ),
            "retest_min_excursion_atr": params.get(
                "retest_min_excursion_atr", DEFAULT_RETEST_MIN_EXCURSION_ATR
            ),
            "retest_max_bars": params.get(
                "retest_max_bars", DEFAULT_RETEST_MAX_BARS
            ),
            "retest_atr_period": params.get(
                "retest_atr_period", DEFAULT_RETEST_ATR_PERIOD
            ),
            "retest_touch_tolerance_atr": params.get(
                "retest_touch_tolerance_atr", DEFAULT_RETEST_TOUCH_TOLERANCE_ATR
            ),
            "retest_max_penetration_atr": params.get(
                "retest_max_penetration_atr", DEFAULT_RETEST_MAX_PENETRATION_ATR
            ),
            "retest_hold_confirm_bars": params.get(
                "retest_hold_confirm_bars", DEFAULT_RETEST_HOLD_CONFIRM_BARS
            ),
        }
        self._signal_state = BreakoutRetestStateMachine(
            **self._signal_state_params,
        )
        self._gap_rewarm_remaining = 0

    def handle_gap(
        self,
        *,
        policy: str,
        gap: Mapping[str, Any],
        next_bar_time: datetime,
        rewarm_bars: int,
    ) -> Mapping[str, Any]:
        normalized = str(policy or "").strip().lower()
        if normalized == "reject":
            raise RuntimeError(
                "market_profile_gap_rejected: "
                f"indicator_id={self._indicator_id} gap={dict(gap)}"
            )
        if normalized == "continue_degraded":
            return {
                "indicator_id": self._indicator_id,
                "policy": normalized,
                "action": "continued_degraded",
                "next_bar_time": next_bar_time.isoformat(),
            }
        if normalized != "reset_rewarm":
            raise RuntimeError(
                "market_profile_gap_policy_invalid: "
                f"indicator_id={self._indicator_id} policy={normalized}"
            )
        self._previous_profile_key = None
        self._previous_location = None
        self._signal_state = BreakoutRetestStateMachine(
            **self._signal_state_params
        )
        self._gap_rewarm_remaining = max(
            int(rewarm_bars or 0),
            int(self._signal_state_params["retest_atr_period"]),
        )
        self._reset_outputs(next_bar_time)
        return {
            "indicator_id": self._indicator_id,
            "policy": normalized,
            "action": "reset_and_rewarm",
            "rewarm_bars": self._gap_rewarm_remaining,
            "next_bar_time": next_bar_time.isoformat(),
        }

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
        if self._source_candle_continuity:
            self._current_transform_summary["source_candle_continuity"] = (
                self._source_continuity_context()
            )
            self._log_source_continuity_warning_once()
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
        additional_signal_events = self._signal_state.step(bar_state)
        if self._gap_rewarm_remaining > 0:
            self._gap_rewarm_remaining -= 1
            if self._gap_rewarm_remaining == 0:
                self._signal_state.clear_sequence()
            self._outputs = build_not_ready_outputs(bar.time)
            return
        self._outputs = build_market_profile_outputs(
            bar_state,
            additional_signal_events=additional_signal_events,
        )
        self._log_signal_events(bar_state=bar_state)

    def _reset_outputs(self, bar_time: datetime) -> None:
        self._current_effective_profiles = []
        self._current_overlay_summary = {}
        self._overlay_ready = False
        self._outputs = build_not_ready_outputs(bar_time)
        self._previous_profile_key = None
        self._previous_location = None

    def _log_signal_events(self, *, bar_state: Any) -> None:
        for output_name in (
            "balance_breakout",
            "confirmed_balance_breakout",
            "balance_reclaim",
            "balance_retest",
        ):
            runtime_output = self._outputs.get(output_name)
            events = runtime_output.value.get("events") if runtime_output is not None else None
            if not isinstance(events, list):
                continue
            for event in events:
                if not isinstance(event, Mapping):
                    continue
                metadata = event.get("metadata")
                metadata_map = metadata if isinstance(metadata, Mapping) else {}
                reference = metadata_map.get("reference")
                reference_map = reference if isinstance(reference, Mapping) else {}
                log.info(
                    "event=market_profile_signal_emitted indicator_id=%s symbol=%s bar_time=%s output_name=%s event_key=%s direction=%s pattern_id=%s profile_key=%s level_name=%s level_price=%s trigger_price=%s active_vah=%s active_val=%s active_poc=%s",
                    self._indicator_id,
                    self._symbol or None,
                    self._current_bar_time.isoformat(),
                    output_name,
                    event.get("key"),
                    event.get("direction"),
                    event.get("pattern_id"),
                    reference_map.get("key") or bar_state.active_profile_key,
                    reference_map.get("label") or reference_map.get("name"),
                    reference_map.get("price"),
                    metadata_map.get("trigger_price"),
                    bar_state.vah,
                    bar_state.val,
                    bar_state.poc,
                )

    def _source_continuity_context(self) -> dict[str, Any]:
        payload = dict(self._source_candle_continuity or {})
        continuity = dict(payload.get("continuity") or {})
        gaps = continuity.get("gaps")
        if isinstance(gaps, list):
            continuity["gaps"] = [dict(gap) for gap in gaps[:3] if isinstance(gap, Mapping)]
        payload["continuity"] = continuity
        return payload

    def _source_continuity_needs_warning(self) -> bool:
        if not self._source_candle_continuity:
            return False
        status = str(self._source_candle_continuity.get("status") or "").lower()
        severity = str(self._source_candle_continuity.get("severity") or "").lower()
        return status not in {"", "ok"} or severity not in {"", "ok", "info"}

    def _log_source_continuity_warning_once(self) -> None:
        if self._source_continuity_warning_logged:
            return
        if not self._source_continuity_needs_warning():
            return
        self._source_continuity_warning_logged = True
        continuity = dict(self._source_candle_continuity.get("continuity") or {})
        gaps = continuity.get("gaps")
        first_gap = gaps[0] if isinstance(gaps, list) and gaps else None
        log.warning(
            "event=market_profile_source_continuity_caveat indicator_id=%s symbol=%s status=%s severity=%s acceptability=%s timeframe=%s final_status=%s detected_gap_count=%s defect_gap_count=%s missing_candle_estimate=%s first_gap=%s",
            self._indicator_id,
            self._symbol or None,
            self._source_candle_continuity.get("status"),
            self._source_candle_continuity.get("severity"),
            self._source_candle_continuity.get("acceptability"),
            self._source_candle_continuity.get("timeframe"),
            continuity.get("final_status"),
            continuity.get("detected_gap_count"),
            continuity.get("defect_gap_count"),
            continuity.get("missing_candle_estimate"),
            first_gap,
        )

    def runtime_diagnostics(self) -> Mapping[str, Any]:
        if not self._source_candle_continuity:
            return {}
        return {
            "source_candle_continuity": self._source_continuity_context(),
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
