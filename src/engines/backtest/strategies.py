"""Pluggable strategies for ladder risk sizing and stop/target construction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Protocol, Sequence

from risk import price_from_r, r_value_from_atr, ticks_for_r, ticks_from_entry

from .configuration import InstrumentConfig, RiskConfig
from .models import Leg
from .utils import coerce_float


@dataclass
class StopComputation:
    """Stop and risk context derived from entry candle."""

    stop_price: float
    r_value: Optional[float]
    r_ticks: Optional[float]
    one_r_distance: Optional[float]
    atr_at_entry: Optional[float]


@dataclass
class StopTargetResult(StopComputation):
    """Outcome of a stop/target build step."""

    legs: List[Leg]
    stop_adjustments: List[Dict[str, object]]


class RiskSizingStrategy(Protocol):
    """Calculate order size for a given risk distance."""

    def contracts_from_effective_risk(
        self, stop_distance_price: float, risk: RiskConfig, instrument: InstrumentConfig
    ) -> Optional[float]:
        ...


class StopTargetStrategy(Protocol):
    """Build stops and targets for a laddered position."""

    def compute_stop(
        self, candle, direction: str, risk: RiskConfig
    ) -> StopComputation:
        ...

    def build_targets(
        self,
        candle,
        direction: str,
        risk: RiskConfig,
        orders: Sequence[Dict[str, object]],
        stop_info: StopComputation,
    ) -> StopTargetResult:
        ...


class DefaultRiskSizingStrategy:
    """Risk sizing using configured base risk and instrument metadata."""

    def contracts_from_effective_risk(
        self, stop_distance_price: float, risk: RiskConfig, instrument: InstrumentConfig
    ) -> Optional[float]:
        if risk.base_risk_per_trade in (None, ""):
            return None
        try:
            base_risk = max(float(risk.base_risk_per_trade), 0.0)
        except (TypeError, ValueError):
            return None
        effective_multiplier = max(risk.global_risk_multiplier or 1.0, 0.0) * max(
            risk.instrument_risk_multiplier or 1.0, 0.0
        )
        effective_risk = base_risk * (effective_multiplier or 1.0)
        if effective_risk <= 0:
            return None

        if instrument.tick_value not in (None, 0) and risk.tick_size not in (None, 0):
            ticks = abs(stop_distance_price) / abs(risk.tick_size)
            risk_per_unit = ticks * abs(instrument.tick_value)
        else:
            risk_per_unit = abs(stop_distance_price) * abs(instrument.point_value())
        if risk_per_unit == 0:
            return None

        qty = effective_risk / risk_per_unit
        return instrument.apply_quantity_constraints(qty)


class DefaultStopTargetStrategy:
    """Replicate legacy stop/target logic for backtests."""

    def compute_stop(
        self, candle, direction: str, risk: RiskConfig
    ) -> StopComputation:
        atr_at_entry = getattr(candle, "atr", None)
        r_value = r_value_from_atr(atr_at_entry, risk.r_multiple)
        r_ticks = ticks_for_r(r_value, risk.tick_size)
        stop_price = None
        one_r_distance = None
        stop_distance_price = None

        if risk.risk_unit_mode == "ticks":
            stop_distance_price = abs(risk.ticks_stop * risk.tick_size)
            stop_price = (
                candle.close - stop_distance_price if direction == "long" else candle.close + stop_distance_price
            )
            one_r_distance = stop_distance_price
            r_value = one_r_distance
            r_ticks = ticks_for_r(one_r_distance, risk.tick_size)
        else:
            explicit_stop = coerce_float(risk.stop_price)
            if explicit_stop not in (None, 0):
                stop_price = float(explicit_stop)
            if risk.stop_r_multiple is not None and r_value is not None and stop_price is None:
                stop_price = price_from_r(candle.close, direction, r_value, risk.stop_r_multiple)
            if stop_price is None:
                stop_distance = risk.stop_ticks * risk.tick_size
                stop_price = (
                    candle.close - stop_distance if direction == "long" else candle.close + stop_distance
                )
            stop_distance_price = abs(candle.close - stop_price)
            one_r_distance = stop_distance_price if stop_distance_price is not None else r_value
            if r_value is None:
                r_value = one_r_distance
            if r_ticks is None:
                r_ticks = ticks_for_r(r_value, risk.tick_size)

        return StopComputation(
            stop_price=stop_price,
            r_value=r_value,
            r_ticks=r_ticks,
            one_r_distance=one_r_distance,
            atr_at_entry=atr_at_entry,
        )

    def _build_stop_adjustments(
        self, legs: Sequence[Leg], r_ticks: Optional[float], config: RiskConfig
    ) -> List[Dict[str, Any]]:
        runtime_rules: List[Dict[str, Any]] = []
        if r_ticks in (None, 0):
            return runtime_rules

        for idx, entry in enumerate(config.stop_adjustments):
            trigger_type = str(entry.get("trigger_type") or "").lower()
            action_type = str(entry.get("action_type") or "").lower()
            if trigger_type not in {"r_multiple", "target_hit"}:
                continue
            if action_type not in {"move_to_breakeven", "move_to_r"}:
                continue

            trigger_ticks: Optional[float] = None
            trigger_target_id: Optional[str] = None

            if trigger_type == "r_multiple":
                trigger_value = coerce_float(entry.get("trigger_value"))
                if trigger_value in (None, 0) or trigger_value <= 0:
                    continue
                trigger_ticks = float(trigger_value) * float(r_ticks)
            else:
                desired = entry.get("trigger_value") or entry.get("trigger_target_id") or entry.get("target_id")
                if desired is None:
                    continue
                for leg in legs:
                    if str(leg.leg_id or leg.name) == str(desired):
                        trigger_target_id = leg.leg_id or leg.name
                        break
                if trigger_target_id is None:
                    continue

            action_r: Optional[float] = None
            if action_type == "move_to_r":
                action_r = coerce_float(entry.get("action_value"))
                if action_r in (None, 0) or action_r <= 0:
                    continue

            runtime_rules.append(
                {
                    "id": entry.get("id") or f"sa-{idx + 1}",
                    "trigger_type": trigger_type,
                    "trigger_ticks": trigger_ticks,
                    "trigger_target_id": trigger_target_id,
                    "action_type": action_type,
                    "action_r": action_r,
                    "fired": False,
                }
            )

        return runtime_rules

    def build_targets(
        self,
        candle,
        direction: str,
        risk: RiskConfig,
        orders: Sequence[Dict[str, object]],
        stop_info: StopComputation,
    ) -> StopTargetResult:
        legs: List[Leg] = []
        for idx, order in enumerate(orders):
            ticks = order.get("ticks")
            r_multiple = order.get("r_multiple")
            price = order.get("price")
            target_price: Optional[float] = None
            target_ticks: Optional[int] = ticks if ticks is not None else None
            if r_multiple is not None and stop_info.r_value is not None:
                target_price = price_from_r(candle.close, direction, stop_info.r_value, r_multiple)
                computed_ticks = ticks_from_entry(candle.close, target_price, direction, risk.tick_size)
                target_ticks = int(round(computed_ticks))
            elif ticks is not None:
                distance = ticks * risk.tick_size
                target_price = candle.close + distance if direction == "long" else candle.close - distance
            elif price is not None:
                target_price = float(price)
                computed_ticks = ticks_from_entry(candle.close, target_price, direction, risk.tick_size)
                target_ticks = int(round(computed_ticks))
            if target_price is None:
                continue
            legs.append(
                Leg(
                    name=order.get("label") or f"TP{target_ticks or ticks or idx + 1}",
                    ticks=target_ticks or 0,
                    target_price=target_price,
                    contracts=order.get("contracts", 1),
                    leg_id=order.get("id") or order.get("label") or f"tp-{idx + 1}",
                )
            )

        runtime_stop_adjustments = self._build_stop_adjustments(legs, stop_info.r_ticks, risk)
        return StopTargetResult(
            stop_price=stop_info.stop_price,
            r_value=stop_info.r_value,
            r_ticks=stop_info.r_ticks,
            one_r_distance=stop_info.one_r_distance,
            atr_at_entry=stop_info.atr_at_entry,
            legs=legs,
            stop_adjustments=runtime_stop_adjustments,
        )
